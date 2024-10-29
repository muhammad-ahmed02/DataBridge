from django.shortcuts import render, redirect, reverse, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponse
import boto3 as bt
import snowflake.connector

from .forms import *
from .models import S3Object
from .utils import *


@login_required
def init_view(request):
    """
    View includes page for getting S3 Access and Secret Key to initialize the further operations.
    """
    s3_objs = S3Object.objects.filter(client=request.user.id)
    if request.method == 'POST':
        form = InitializeForm(request.POST)
        if form.is_valid():
            access_key = form.cleaned_data["access_key"]
            secret_key = form.cleaned_data["secret_key"]
            s3_client = bt.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            if bool(s3_client.list_buckets()):
                obj = S3Object.objects.create(client=request.user.id, access_key=access_key, secret_key=secret_key)
                redirect_url = reverse("aws:bucket", args=[obj.id])
                return redirect(redirect_url)
    else:
        form = InitializeForm()
    args = {'form': form, "s3_objs": s3_objs}
    return render(request, "aws/init.html", args)


@login_required
def bucket_view(request, obj_id):
    """
    View to save bucket name from the input of user into S3Object and performing further operations.
    returns: List buckets from S3 User given in InitView.
    """
    obj = S3Object.objects.get(id=obj_id)
    s3_client = bt.client('s3', aws_access_key_id=obj.access_key, aws_secret_access_key=obj.secret_key)
    response = s3_client.list_buckets()
    buckets = [(str(bucket['Name'].lower()), str(bucket['Name'])) for bucket in response['Buckets']]

    if request.method == "POST":
        form = BucketForm(request.POST)
        form.set_buckets(buckets)
        if form.is_valid():
            obj.bucket_name = form.cleaned_data['bucket_name']
            obj.extension = str(form.cleaned_data['extension'].ext)
            obj.save()
            if str(form.cleaned_data['type']).lower() == 'file':
                redirect_url = reverse('aws:files', args=[obj.id])
            else:
                redirect_url = reverse('aws:folders', args=[obj.id])

            return redirect(redirect_url)
        else:
            errors = form.errors
            return render(request, 'aws/bucket.html', {'form': form, 'errors': errors})
    else:
        form = BucketForm()
        form.set_buckets(buckets)
    args = {'form': form}
    return render(request, "aws/bucket.html", args)


@login_required
def files_view(request, obj_id):
    """
    View for files display from S3 bucket selected in BucketView.
    """
    obj = S3Object.objects.get(id=obj_id)
    s3_client = bt.client('s3', aws_access_key_id=obj.access_key, aws_secret_access_key=obj.secret_key)
    files = find_files_in_s3(s3_client, obj.bucket_name, obj.extension)

    if request.method == "POST":
        if 'ext' in request.POST:
            extension_form = ExtensionSelect(request.POST)
            if extension_form.is_valid():
                form = FilesForm()
                ext = extension_form.cleaned_data['ext'].ext
                files = find_files_in_s3(s3_client, obj.bucket_name, ext)
                form.set_files(files)
                obj.extension = ext
                obj.save()
                return render(request, "aws/files.html",
                              {'form': form, 'extension_form': extension_form, 'obj': obj})
        else:
            extension_form = ExtensionSelect(initial={'ext': obj.extension})

        form = FilesForm(request.POST)
        form.set_files(files)
        if form.is_valid():
            table = form.cleaned_data['file_name']
            obj.file_name = table
            obj.save()

            if 'get_schema' in request.POST:
                cond, df = get_file_df(con=s3_client, bucket=obj.bucket_name, table=table)
                schema = get_schema(df, table) if cond else "None"
                if cond:
                    return render(request, "aws/files.html",
                                  {'form': form, 'extension_form': extension_form, 'obj': obj, 'schema': schema})
                else:
                    error = df
                    return render(request, "aws/files.html",
                                  {"form": form, 'extension_form': extension_form, 'obj': obj, 'error': error})
            elif "export_schema" in request.POST:
                cond, df = get_file_df(con=s3_client, bucket=obj.bucket_name, table=table)
                schema = get_schema(df, table) if cond else "None"
                if cond:
                    response = JsonResponse(schema)
                    response['Content-Disposition'] = 'attachment; filename="schema.json"'
                    return response
            elif "write_to_target" in request.POST:
                cond, df = get_file_df(con=s3_client, bucket=obj.bucket_name, table=table)
                if cond:
                    redirect_url = reverse('aws:target_select', args=[obj.id, "files"])
                    return redirect(redirect_url)
                else:
                    error = df
                    return render(request, "aws/files.html",
                                  {"form": form, 'extension_form': extension_form, 'obj': obj, 'error': error})
        else:
            errors = form.errors
            return render(request, "aws/files.html",
                          {'form': form, 'extension_form': extension_form, 'errors': errors, 'obj': obj})
    else:
        form = FilesForm()
        form.set_files(files)
        extension_form = ExtensionSelect(initial={'ext': obj.extension})

    args = {'form': form, 'extension_form': extension_form, 'obj': obj}
    return render(request, "aws/files.html", args)


@login_required
def folders_view(request, obj_id):
    obj = S3Object.objects.get(id=obj_id)
    s3_client = bt.client('s3', aws_access_key_id=obj.access_key, aws_secret_access_key=obj.secret_key)
    responses = s3_client.list_objects_v2(Bucket=obj.bucket_name)

    folders = list()
    for response in responses.get("Contents", []):
        var = response["Key"]
        var_parts = var.split("/")
        if len(var_parts) > 1:
            # Get the top-level folder
            if not has_special_characters(var_parts[0]):
                top_level_folder = var_parts[0]
                # Add the top-level folder name to the set
                new_folder = make_tuple(str(top_level_folder), size=response['Size'])
                if check_unique_tuple(new_folder, folders):
                    folders.append(new_folder)

    if request.method == "POST":
        if 'ext' in request.POST:
            extension_form = ExtensionSelect(request.POST)
            if extension_form.is_valid():
                form = FoldersForm()
                form.set_folders(folders)
                ext = extension_form.cleaned_data['ext'].ext
                obj.extension = ext
                obj.save()
                return render(request, "aws/folders.html",
                              {'form': form, 'extension_form': extension_form, 'obj': obj})
        else:
            extension_form = ExtensionSelect(initial={'ext': obj.extension})

        form = FoldersForm(request.POST)
        form.set_folders(folders)
        if form.is_valid():
            folder = form.cleaned_data['folder_name']
            obj.folder_name = folder
            obj.save()
            if 'get_schema' in request.POST:
                cond, schema = get_folder_schema_in_s3(s3_client, obj.bucket_name, folder, obj.extension)
                if cond:
                    return render(request, "aws/folders.html",
                                  {'form': form, 'extension_form': extension_form, 'obj': obj, 'schema': schema})
                error = schema
                return render(request, "aws/folders.html",
                              {'form': form, 'extension_form': extension_form, 'obj': obj, 'error': error})
            elif "export_schema" in request.POST:
                cond, schema = get_folder_schema_in_s3(s3_client, obj.bucket_name, folder, obj.extension)
                if cond:
                    response = JsonResponse(schema)
                    response['Content-Disposition'] = 'attachment; filename="schema.json"'
                    return response
            elif "write_to_target" in request.POST:
                cond, schema = get_folder_schema_in_s3(s3_client, obj.bucket_name, folder, obj.extension)
                if cond:
                    redirect_url = reverse('aws:target_select', args=[obj.id, "folders"])
                    return redirect(redirect_url)
                error = schema
                return render(request, "aws/folders.html",
                              {'form': form, 'extension_form': extension_form, 'obj': obj, 'error': error})
        else:
            errors = form.errors
            return render(request, "aws/folders.html",
                          {'form': form, 'extension_form': extension_form, 'errors': errors})
    else:
        form = FoldersForm()
        form.set_folders(folders)
        extension_form = ExtensionSelect(initial={'ext': obj.extension})

    args = {'form': form, 'extension_form': extension_form, 'obj': obj}
    return render(request, "aws/folders.html", args)


@login_required
def target_select_view(request, obj_id, types):
    target_list = SnowflakeObject.objects.filter(client=request.user.id)
    s3_obj = S3Object.objects.get(id=obj_id)
    if request.method == 'POST':
        form = TargetForm(request.POST)
        if form.is_valid():
            # Process the form data
            obj = form.save()
            return redirect(reverse("aws:target_writing", args=[s3_obj.id, types, obj.id]))
    else:
        form = TargetForm()
    args = {"target_list": target_list, 'form': form, 'types': types, 'obj': s3_obj}
    return render(request, 'aws/target.html', args)


@login_required
def edit_target(request, target_id, obj_id, types):
    target = get_object_or_404(SnowflakeObject, id=target_id)
    s3_obj = S3Object.objects.get(id=obj_id)
    if request.method == 'POST':
        form = TargetForm(request.POST, instance=target)
        if form.is_valid():
            form.save()
            return redirect(reverse("aws:target_select", args=[s3_obj.id, types]))
    else:
        form = TargetForm(instance=target)
    args = {'form': form, 'obj': s3_obj, 'types': types, 'target': target}
    return render(request, 'aws/target_edit.html', args)


@login_required
def target_writing_view(request, obj_id, target_id, types):
    """
    In this page we will perform complete ETL process.
        1. Extract: get the table data from S3
        2. Transform: Get the table schema and transform the data in order to write in Snowflake.
        3. Load: Create Table in Snowflake with schema columns and load the transformed data.
    """
    obj = S3Object.objects.get(id=obj_id)
    target = SnowflakeObject.objects.get(id=target_id)

    args = {'target': target, 'obj': obj, 'types': types}
    return render(request, "aws/target_writing.html", args)


def extract_table(request, types, obj_id, target_id):
    obj = S3Object.objects.get(id=obj_id)
    target = SnowflakeObject.objects.get(id=target_id)

    s3_client = bt.client('s3', aws_access_key_id=obj.access_key, aws_secret_access_key=obj.secret_key)
    if types == "files":
        source = obj.file_name
        cond, df = get_file_df(con=s3_client, bucket=obj.bucket_name, table=source)
        schema = get_schema(df, source) if cond else {source: "Not Found!"}
    else:
        source = obj.folder_name
        cond, schema = get_folder_schema_in_s3(s3_client, obj.bucket_name, source, obj.extension)
        if not cond:
            error = "Schema not found!"
            return JsonResponse({'error': str(error)}, status=500)
        cond, df = get_folder_df(s3_client, obj.bucket_name, source, obj.extension)

    try:
        # creating table on snowflake
        conn = snowflake.connector.connect(
            account=target.sfAccount,
            user=target.sfUser,
            password=target.sfPassword,
            database=target.sfDatabase,
            schema=target.sfSchema,
            warehouse=target.sfWarehouse,
        )

        conn.cursor().execute(f'USE DATABASE {target.sfDatabase}')
        conn.cursor().execute(f'USE SCHEMA {target.sfSchema}')

        if types == "files":
            db_table = obj.file_name.replace(".", "_")
        else:
            db_table = obj.folder_name
        columns = convert_schema_sql(schema=schema)

        # Create a new table
        create_table_command = f'CREATE TABLE IF NOT EXISTS {db_table} ({columns})'
        conn.cursor().execute(create_table_command)

        # Write the DataFrame to Snowflake using INSERT query
        placeholders = ', '.join(['%s'] * len(df.columns))
        query = f"INSERT INTO {db_table} ({', '.join(df.columns)}) VALUES ({placeholders})"
        conn.cursor().executemany(query, df.values.tolist())
        conn.close()
        return JsonResponse(schema)
    except Exception as e:
        error = f"Error: {e}"
        return JsonResponse({'error': str(error)}, status=500)
