from django.urls import path

from aws.views import *

app_name = "aws"

urlpatterns = [
    path('', init_view, name="init"),
    path('<int:obj_id>/bucket/', bucket_view, name="bucket"),
    path('<int:obj_id>/files/', files_view, name="files"),
    path('<int:obj_id>/folders/', folders_view, name="folders"),
    path('<int:obj_id>/<str:types>/target/', target_view, name="target")
]
