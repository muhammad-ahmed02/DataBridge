from django.contrib import admin
from django.contrib.auth.models import User
from django.contrib.auth.admin import UserAdmin
from .models import S3Object, TypeModel, Extension, SnowflakeObject, Client, ClientUserProfile


class ClientUserProfileAdminInline(admin.StackedInline):
    """
    This inline admin config allow a user to associate a client with a user from the backend panel
    """
    model = ClientUserProfile
    can_delete = False


class AppUserAdmin(UserAdmin):
    """
    This admin class extends the user admin setup to include the client user profile as a
    configurable option.
    """
    inlines = (ClientUserProfileAdminInline,)
    list_display = ('username', 'email', 'first_name', 'last_name', 'is_staff')


class S3ObjectAdmin(admin.ModelAdmin):
    list_display = ['client', 'access_key', 'secret_key']
    list_filter = ('access_key', 'secret_key')


class SnowflakeAdmin(admin.ModelAdmin):
    list_display = ['client', 'sfUrl', 'sfAccount']


admin.site.register(S3Object, S3ObjectAdmin)
admin.site.register(TypeModel)
admin.site.register(Extension)
admin.site.register(SnowflakeObject, SnowflakeAdmin)
admin.site.register(Client)
admin.site.unregister(User)
admin.site.register(User, AppUserAdmin)
