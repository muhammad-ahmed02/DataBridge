from django import forms


class InitializeForm(forms.Form):
    access_key = forms.CharField(label='Access Key')
    secret_key = forms.CharField(label='Secret Key')


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
    bucket_name = forms.CharField(label='Bucket Name')
    type = forms.ChoiceField(label="File/Folder", choices=type_choices, widget=forms.RadioSelect)
    extension = forms.ChoiceField(label="Extension", choices=extensions, required=False)


class FilesForm(forms.Form):
    file_name = forms.CharField(label='File Name')


class FoldersForm(forms.Form):
    folder_name = forms.CharField(label="Folder Name")
