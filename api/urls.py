"""
URL patterns for the API app
"""
from django.urls import path
from . import views

urlpatterns = [
    path('organizations/', views.OrganizationListView.as_view(), name='organization-list'),
    path('organizations/<str:org_id>/', views.OrganizationDetailView.as_view(), name='organization-detail'),
    path('industries/', views.IndustryListView.as_view(), name='industry-list'),
    path('keywords/', views.KeywordListView.as_view(), name='keyword-list'),
]