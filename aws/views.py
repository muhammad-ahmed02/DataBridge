from django.shortcuts import render, redirect, reverse
from django.http import JsonResponse
from .forms import *
from .models import S3Object
from .utils import *
import boto3 as bt


def init_view(request):
    if request.method == 'POST':
        form = InitializeForm(request.POST)
        if form.is_valid():
            access_key = form.cleaned_data["access_key"]
            secret_key = form.cleaned_data["secret_key"]
            s3_client = bt.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            if bool(s3_client.list_buckets()):
                obj = S3Object.objects.create(access_key=access_key, secret_key=secret_key)
                redirect_url = reverse("aws:bucket", args=[obj.id])
                return redirect(redirect_url)
    else:
        form = InitializeForm()
    args = {'form': form}
    return render(request, "aws/init.html", args)


def bucket_view(request, obj_id):
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


def find_files_in_s3(con, bucket, ext) -> list:
    responses = con.list_objects_v2(Bucket=bucket)

    files = list()
    for response in responses.get("Contents", []):
        var = response["Key"]
        var_parts = var.split("/")
        if len(var_parts) > 1:
            continue
        else:
            if var.endswith(ext):
                files.append(make_tuple(var, size=response['Size']))
    return files


def files_view(request, obj_id):
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
            cond, df = get_file_df(con=s3_client, bucket=obj.bucket_name, table=table)
            schema = get_schema(df, table) if cond else "None"
            if 'get_schema' in request.POST:
                if cond:
                    return render(request, "aws/files.html",
                                  {'form': form, 'extension_form': extension_form, 'obj': obj, 'schema': schema})
                else:
                    error = df
                    return render(request, "aws/files.html",
                                  {"form": form, 'extension_form': extension_form, 'obj': obj, 'error': error})
            elif "export_schema" in request.POST:
                if cond:
                    response = JsonResponse(schema)
                    response['Content-Disposition'] = 'attachment; filename="schema.json"'
                    return response
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


def find_folder_in_s3(con, bucket, table, ext) -> tuple:
    files = con.list_objects_v2(Bucket=bucket, Prefix=table)
    for file in sorted(files.get("Contents", []), key=lambda x: x['LastModified']):
        if file['Key'].endswith(ext):
            cond, df = get_file_df(con, bucket, table=file['Key'])
            if cond:
                schema = get_schema(df, file['Key'])
                return cond, schema
    return False, "File too large to get schema! Try with files less than 1MB"


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
        form = FoldersForm(request.POST)
        form.set_folders(folders)
        if form.is_valid():
            folder = form.cleaned_data['folder_name']
            obj.folder_name = folder
            obj.save()
            cond, schema = find_folder_in_s3(s3_client, obj.bucket_name, folder, obj.extension)
            if 'get_schema' in request.POST:
                if cond:
                    return render(request, "aws/folders.html",
                                  {'form': form, 'obj': obj, 'schema': schema})
                error = schema
                return render(request, "aws/folders.html", {'form': form, 'obj': obj, 'error': error})
            elif "export_schema" in request.POST:
                if cond:
                    response = JsonResponse(schema)
                    response['Content-Disposition'] = 'attachment; filename="schema.json"'
                    return response
        else:
            errors = form.errors
            return render(request, "aws/folders.html", {'form': form, 'errors': errors})
    else:
        form = FoldersForm()
        form.set_folders(folders)
    args = {'form': form, 'obj': obj}
    return render(request, "aws/folders.html", args)
