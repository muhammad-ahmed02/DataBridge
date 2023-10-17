from django import forms
from .models import TypeModel, Extension


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


class BucketForm(forms.Form):
    bucket_name = forms.ChoiceField(label='Bucket Name:',
                                    choices=[],
                                    widget=forms.Select(attrs={'class': 'form-select'}))
    type = forms.ModelChoiceField(label="File/Folder:",
                                  queryset=TypeModel.objects.all(),
                                  to_field_name='name',
                                  widget=forms.RadioSelect(attrs={'div_class': 'form-check',
                                                                  'input_class': 'form-check-input'}))
    extension = forms.ModelChoiceField(label="Extension:",
                                       queryset=Extension.objects.all(),
                                       to_field_name='name',
                                       widget=forms.Select(attrs={'class': 'form-select'}),
                                       required=False)

    def set_buckets(self, choices):
        self.fields['bucket_name'].choices = choices


class FilesForm(forms.Form):
    file_name = forms.CharField(label='File Name')


class FoldersForm(forms.Form):
    folder_name = forms.CharField(label="Folder Name")
