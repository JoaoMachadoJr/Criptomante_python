from django.urls import path
from website import views

urlpatterns = [
    path('', views.website, name='website'),
]