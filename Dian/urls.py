from django.urls import path
# from . import views
from .views import *

urlpatterns = [
    path('ejecutar-scraping/', ejecutar_scraping, name='ejecutar_scraping'),
]