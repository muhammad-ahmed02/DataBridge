from django.db import models


class S3Object(models.Model):
    access_key = models.CharField(max_length=255)
    secret_key = models.CharField(max_length=255)
    bucket_name = models.CharField(max_length=64, blank=True, null=True)
    file_name = models.CharField(max_length=255, blank=True, null=True)
    folder_name = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return self.access_key
