from django.urls import path

from aws.views import *

app_name = "aws"

urlpatterns = [
    path('', init_view, name="init"),
    path('<int:obj_id>/bucket/', bucket_view, name="bucket"),
    path('<int:obj_id>/files/', files_view, name="files"),
    path('<int:obj_id>/folders/', folders_view, name="folders"),
    path('<int:obj_id>/<str:types>/target/', target_select_view, name="target_select"),
    path('<int:obj_id>/<str:types>/target/<int:target_id>/', edit_target, name="target_edit"),
    path('<int:obj_id>/<str:types>/target/<int:target_id>/write/', target_writing_view, name="target_writing"),
    path('<int:obj_id>/get-schema/<str:types>/<int:target_id>/', extract_table),
]
