from django.contrib import admin
from .models import S3Object, TypeModel, Extension, SnowflakeObject


class S3ObjectAdmin(admin.ModelAdmin):
    list_display = ['access_key', 'secret_key']
    list_filter = ('access_key', 'secret_key')


admin.site.register(S3Object, S3ObjectAdmin)
admin.site.register(TypeModel)
admin.site.register(Extension)
admin.site.register(SnowflakeObject)
