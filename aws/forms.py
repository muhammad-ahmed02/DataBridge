from django import forms


class InitializeForm(forms.Form):
    access_key = forms.CharField(label='Access Key')
    secret_key = forms.CharField(label='Secret Key')


type_choices = [
    ('file', 'File'),
    ('folder', 'Folder')
]


class BucketForm(forms.Form):
    bucket_name = forms.CharField(label='Bucket Name')
    type = forms.ChoiceField(label="File/Folder", choices=type_choices, widget=forms.RadioSelect)
