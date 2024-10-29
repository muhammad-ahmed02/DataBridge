from django.contrib import admin
from django.urls import path, include, re_path
from django.views.static import serve
from django.conf import settings
from django.conf.urls.static import static
from django.contrib.auth.views import LoginView, LogoutView

LoginView.template_name = 'aws/login.html'
LoginView.success_url = "/"

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('aws.urls', namespace="home")),
    path('login/', LoginView.as_view(), name='login'),
    path('logout/', LogoutView.as_view(), name='logout'),

    # re_path(r"^static/(?P<path>.*)$", serve,
    #         {"document_root": settings.STATIC_ROOT}),
    # re_path(r"^media/(?P<path>.*)$", serve,
    #         {"document_root": settings.MEDIA_ROOT}),
]

urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
