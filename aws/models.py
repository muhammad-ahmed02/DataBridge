from django.db import models


class SnowflakeObject(models.Model):
    sfUrl = models.CharField(max_length=255)
    sfAccount = models.CharField(max_length=255)
    sfUser = models.CharField(max_length=255)
    sfPassword = models.CharField(max_length=255)
    sfDatabase = models.CharField(max_length=255)
    sfSchema = models.CharField(max_length=255)
    sfWarehouse = models.CharField(max_length=255)

    dbTable = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return self.sfAccount


class S3Object(models.Model):
    access_key = models.CharField(max_length=255)
    secret_key = models.CharField(max_length=255)
    bucket_name = models.CharField(max_length=64, blank=True, null=True)
    extension = models.CharField(max_length=12, blank=True, null=True)
    file_name = models.CharField(max_length=255, blank=True, null=True)
    folder_name = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return self.access_key


class TypeModel(models.Model):
    name = models.CharField(max_length=32)

    def __str__(self):
        return self.name


class Extension(models.Model):
    name = models.CharField(max_length=32)
    ext = models.CharField(max_length=12, default=".", help_text="Include '.' in the extension")

    def __str__(self):
        return self.name
