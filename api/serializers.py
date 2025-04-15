"""
Serializers for the API app
"""
from rest_framework import serializers

class ObjectIdField(serializers.Field):
    """Field that handles MongoDB ObjectId serialization"""
    def to_representation(self, value):
        return str(value)
    
    def to_internal_value(self, data):
        return data

class OrganizationSerializer(serializers.Serializer):
    """Serializer for Organization data"""
    id = ObjectIdField(source='_id', read_only=True)
    name = serializers.CharField(required=True, max_length=255)
    website_url = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    linkedin_url = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    facebook_url = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    phone = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    number_of_employees = serializers.IntegerField(required=False, allow_null=True)
    industry = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        allow_empty=True
    )
    keywords = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        allow_empty=True
    )
    apollo_id = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    address = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    created_at = serializers.DateTimeField(required=False, allow_null=True)
    updated_at = serializers.DateTimeField(required=False, allow_null=True)

class IndustrySerializer(serializers.Serializer):
    """Serializer for Industry data"""
    id = ObjectIdField(source='_id', read_only=True)
    name = serializers.CharField(required=True, max_length=255)
    created_at = serializers.DateTimeField(required=False, allow_null=True)
    updated_at = serializers.DateTimeField(required=False, allow_null=True)

class KeywordSerializer(serializers.Serializer):
    """Serializer for Keyword data"""
    id = ObjectIdField(source='_id', read_only=True)
    name = serializers.CharField(required=True, max_length=255)
    created_at = serializers.DateTimeField(required=False, allow_null=True)
    updated_at = serializers.DateTimeField(required=False, allow_null=True)