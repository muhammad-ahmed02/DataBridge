from django.shortcuts import render, redirect, reverse
from django.views.generic import FormView
from .forms import *
from .models import S3Object

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
            obj.extension = form.cleaned_data['extension']
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


def files_view(request, obj_id):
    obj = S3Object.objects.get(id=obj_id)
    s3_client = bt.client('s3', aws_access_key_id=obj.access_key, aws_secret_access_key=obj.secret_key)
    responses = s3_client.list_objects_v2(Bucket=obj.bucket_name)

    files = list()
    for response in responses.get("Contents", []):
        files.append(response['Key'])

    if request.method == "POST":
        form = FilesForm(request.POST)
        if form.is_valid():
            obj.file_name = form.cleaned_data['file_name']
            obj.save()
            return redirect(reverse('aws:files', args=[obj.id]))
    else:
        form = FilesForm()

    args = {'form': form, 'files': files}
    return render(request, "aws/files.html", args)


def folders_view(request, obj_id):
    obj = S3Object.objects.get(id=obj_id)
    s3_client = bt.client('s3', aws_access_key_id=obj.access_key, aws_secret_access_key=obj.secret_key)
    responses = s3_client.list_objects_v2(Bucket=obj.bucket_name)

    folders = list()
    for response in responses.get("Contents", []):
        folders.append(response['Key'])

    if request.method == "POST":
        form = FoldersForm(request.POST)
        if form.is_valid():
            obj.folder_name = form.cleaned_data['folder_name']
            obj.save()
            return redirect(reverse('aws:folder', args=[obj.id]))
    else:
        form = FoldersForm()

    args = {'form': form, 'folders': folders}
    return render(request, "aws/folders.html", args)
