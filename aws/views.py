from django.shortcuts import render, redirect, reverse
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
            obj.bucket_name = str(form.cleaned_data['bucket_name'])
            if str(form.cleaned_data['type']).lower() == 'file':
                obj.extension = str(form.cleaned_data['extension'].ext)
                redirect_url = reverse('aws:files', args=[obj.id])
            else:
                redirect_url = reverse('aws:folders', args=[obj.id])
            obj.save()
            return redirect(redirect_url)
        else:
            errors = form.errors
            return render(request, 'aws/bucket.html', {'form': form, 'errors': errors})
    else:
        form = BucketForm()
        form.set_buckets(buckets)
    args = {'form': form}
    return render(request, "aws/bucket.html", args)


def files_view(request, obj_id):
    obj = S3Object.objects.get(id=obj_id)
    s3_client = bt.client('s3', aws_access_key_id=obj.access_key, aws_secret_access_key=obj.secret_key)
    responses = s3_client.list_objects_v2(Bucket=obj.bucket_name)

    files = list()
    for response in responses.get("Contents", []):
        var = response["Key"]
        var_parts = var.split("/")
        if len(var_parts) > 1:
            continue
        else:
            if var.endswith(obj.extension):
                files.append(make_tuple(var, size=response['Size']))

    if request.method == "POST":
        form = FilesForm(request.POST)
        form.set_files(files)
        if form.is_valid():
            table = form.cleaned_data['file_name']
            obj.file_name = table
            obj.save()
            df = get_file_df(con=s3_client, bucket=obj.bucket_name, table=table)
            schema = get_schema(df=df)
            return render(request, "aws/files.html", {'form': form, 'obj': obj, 'schema': schema})
        else:
            errors = form.errors
            return render(request, "aws/files.html", {'form': form, 'errors': errors, 'obj': obj})
    else:
        form = FilesForm()
        form.set_files(files)
    args = {'form': form, 'obj': obj}
    return render(request, "aws/files.html", args)


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
                folder = make_tuple(str(top_level_folder), size=response['Size'])
                folders.append(
                    folder
                ) if folder not in folders else None

    if request.method == "POST":
        form = FoldersForm(request.POST)
        form.set_folders(folders)
        if form.is_valid():
            folder = form.cleaned_data['folder_name']
            obj.folder_name = folder
            obj.save()
            schema = list()
            files = s3_client.list_objects_v2(Bucket=obj.bucket_name, Prefix=folder)
            for file in sorted(files.get("Contents", []), key=lambda x: x['LastModified']):
                if file['Key'].endswith(obj.extension):
                    df = get_file_df(s3_client, bucket=obj.bucket_name, table=file['Key'])
                    schema = get_schema(df)
                    break

            return render(request, "aws/folders.html", {'form': form, 'obj': obj, 'schema': schema})
        else:
            errors = form.errors
            return render(request, "aws/folders.html", {'form': form, 'errors': errors})
    else:
        form = FoldersForm()
        form.set_folders(folders)
    args = {'form': form, 'obj': obj}
    return render(request, "aws/folders.html", args)
