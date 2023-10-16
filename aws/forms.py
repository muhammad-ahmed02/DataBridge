from django import forms
from .models import S3Object
import boto3 as bt


class InitializeForm(forms.Form):
    access_key = forms.CharField(label='Access Key', widget=forms.TextInput(
        attrs={'class': 'form-control',
               'id': 'access_key',
               'name': 'access_key',
               'placeholder': "Enter your Access Key"}
    ))
    secret_key = forms.CharField(label='Secret Key', widget=forms.TextInput(
        attrs={'class': 'form-control',
               'id': 'secret_key',
               'name': 'secret_key',
               'placeholder': "Enter your Secret Key"}
    ))


type_choices = [
    ('file', 'File'),
    ('folder', 'Folder')
]
extensions = [
    ('txt', 'Text'),
    ('xlsx', 'Excel'),
    ('csv', 'CSV'),
    ('xml', 'XML'),
    ('json', 'JSON'),
    ('avro', 'AVRO'),
    ('parquet', 'PARQUET')
]


class BucketForm(forms.Form):
    bucket_name = forms.ChoiceField(label='Bucket Name',
                                    choices=[],
                                    widget=forms.Select(attrs={'class': 'form-select'}))
    type = forms.ChoiceField(label="File/Folder",
                             choices=type_choices,
                             widget=forms.RadioSelect(attrs={'class': 'form-check'}))
    extension = forms.ChoiceField(label="Extension",
                                  choices=extensions,
                                  widget=forms.Select(attrs={'class': 'form-select'}),
                                  required=False)

    def set_choices(self, choices):
        self.fields['bucket_name'].choices = choices


class FilesForm(forms.Form):
    file_name = forms.CharField(label='File Name')


class FoldersForm(forms.Form):
    folder_name = forms.CharField(label="Folder Name")
