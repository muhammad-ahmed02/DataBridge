from django.db import models
from django.contrib.auth.models import User


class Client(models.Model):
    """
    This model represents a client, or a customer of this app. It can be used to isolate content of
    different clients such that they cannot see each others' content or edit / manage it.
    """
    name = models.CharField(
        max_length=100,
        help_text='Name of client')

    def __str__(self):
        return self.name


class ClientUserProfile(models.Model):
    """
    This model connects the user model to the client model allowing users to be associated with a
    client and limits the data they can interact with to that associated with the client.
    """
    user = models.OneToOneField(
        User, related_name='profile', on_delete=models.CASCADE)
    client = models.ForeignKey(Client, on_delete=models.CASCADE)

    def __str__(self):
        return '"{user}" of "{client}"'.format(user=self.user, client=self.client)


class SnowflakeObject(models.Model):
    client = models.ForeignKey(to=Client, on_delete=models.CASCADE, blank=True, null=True)
    sfUrl = models.CharField(max_length=255)
    sfAccount = models.CharField(max_length=255)
    sfUser = models.CharField(max_length=255)
    sfPassword = models.CharField(max_length=255)
    sfDatabase = models.CharField(max_length=255)
    sfSchema = models.CharField(max_length=255)
    sfWarehouse = models.CharField(max_length=255)

    # dbTable = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return self.sfAccount


class S3Object(models.Model):
    client = models.ForeignKey(to=Client, on_delete=models.CASCADE, blank=True, null=True)
    access_key = models.CharField(max_length=255)
    secret_key = models.CharField(max_length=255)
    bucket_name = models.CharField(max_length=64, blank=True, null=True)
    extension = models.CharField(max_length=12, blank=True, null=True)
    file_name = models.CharField(max_length=255, blank=True, null=True)
    folder_name = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return self.access_key


class TypeModel(models.Model):
    client = models.ForeignKey(to=Client, on_delete=models.CASCADE, blank=True, null=True)
    name = models.CharField(max_length=32)

    def __str__(self):
        return self.name


class Extension(models.Model):
    client = models.ForeignKey(to=Client, on_delete=models.CASCADE, blank=True, null=True)
    name = models.CharField(max_length=32)
    ext = models.CharField(max_length=12, default=".", help_text="Include '.' in the extension")

    def __str__(self):
        return self.name
