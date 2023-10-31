from django import forms
from .models import TypeModel, Extension, SnowflakeObject


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
                                       to_field_name='ext',
                                       widget=forms.Select(attrs={'class': 'form-select'}))

    def set_buckets(self, choices):
        self.fields['bucket_name'].choices = choices


class FilesForm(forms.Form):
    file_name = forms.ChoiceField(label='File Name:',
                                  choices=[],
                                  widget=forms.RadioSelect(attrs={'div_class': 'form-check',
                                                                  'input_class': 'form-check-input'}))

    def set_files(self, choices):
        self.fields['file_name'].choices = choices


class FoldersForm(forms.Form):
    folder_name = forms.ChoiceField(label="Folder Name:",
                                    choices=[],
                                    widget=forms.RadioSelect(attrs={'div_class': 'form-check',
                                                                    'input_class': 'form-check-input'}))

    def set_folders(self, choices):
        self.fields['folder_name'].choices = choices


class ExtensionSelect(forms.Form):
    ext = forms.ModelChoiceField(label="Extension:",
                                 queryset=Extension.objects.all(),
                                 to_field_name='ext',
                                 widget=forms.Select(attrs={'class': 'badge bg-secondary',
                                                            'onchange': 'this.form.submit();'}))


class TargetForm(forms.ModelForm):
    class Meta:
        model = SnowflakeObject
        fields = ['sfUrl', 'sfAccount', 'sfUser', 'sfPassword',
                  'sfDatabase', 'sfSchema', 'sfWarehouse']

        widgets = {
            'sfUrl': forms.TextInput(attrs={'class': 'form-control', 'placeholder': "Snowflake Url"}),
            'sfAccount': forms.TextInput(attrs={'class': 'form-control', 'placeholder': "Account name"}),
            'sfUser': forms.TextInput(attrs={'class': 'form-control', 'placeholder': "Username"}),
            'sfPassword': forms.TextInput(attrs={'class': 'form-control', 'placeholder': "Password"}),
            'sfDatabase': forms.TextInput(attrs={'class': 'form-control', 'placeholder': "Database"}),
            'sfSchema': forms.TextInput(attrs={'class': 'form-control', 'placeholder': "Schema"}),
            'sfWarehouse': forms.TextInput(attrs={'class': 'form-control', 'placeholder': "Warehouse"}),
        }
