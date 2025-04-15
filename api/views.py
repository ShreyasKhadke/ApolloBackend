"""
Views for the API app
"""
from rest_framework import views, status
from rest_framework.response import Response
from django.conf import settings
from bson import ObjectId, errors
from .models import Organization, Industry, Keyword
from .serializers import OrganizationSerializer, IndustrySerializer, KeywordSerializer

class OrganizationListView(views.APIView):
    """API view for listing and creating organizations"""
    
    def get(self, request):
        """Get all organizations with pagination"""
        page_size = int(request.query_params.get('page_size', 10))
        page = int(request.query_params.get('page', 1))
        
        skip = (page - 1) * page_size
        organizations = Organization.get_all(limit=page_size, skip=skip)
        
        # Get total count for pagination info
        total_count = Organization.collection.count_documents({})
        
        # Serialize data
        serializer = OrganizationSerializer(organizations, many=True)
        
        return Response({
            'count': total_count,
            'next': f"?page={page+1}&page_size={page_size}" if skip + page_size < total_count else None,
            'previous': f"?page={page-1}&page_size={page_size}" if page > 1 else None,
            'results': serializer.data
        })
    
    def post(self, request):
        """Create a new organization"""
        serializer = OrganizationSerializer(data=request.data)
        
        if serializer.is_valid():
            org_id = Organization.create(serializer.validated_data)
            return Response({'id': str(org_id)}, status=status.HTTP_201_CREATED)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class OrganizationDetailView(views.APIView):
    """API view for retrieving, updating and deleting a specific organization"""
    
    def get_object(self, org_id):
        """Helper method to get organization by ID"""
        try:
            return Organization.get_by_id(org_id)
        except errors.InvalidId:
            return None
    
    def get(self, request, org_id):
        """Get organization by ID"""
        organization = self.get_object(org_id)
        
        if not organization:
            return Response(
                {'error': 'Organization not found'}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        serializer = OrganizationSerializer(organization)
        return Response(serializer.data)
    
    def put(self, request, org_id):
        """Update organization by ID"""
        organization = self.get_object(org_id)
        
        if not organization:
            return Response(
                {'error': 'Organization not found'}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        serializer = OrganizationSerializer(data=request.data)
        
        if serializer.is_valid():
            Organization.update(org_id, serializer.validated_data)
            return Response(serializer.validated_data)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    def delete(self, request, org_id):
        """Delete organization by ID"""
        organization = self.get_object(org_id)
        
        if not organization:
            return Response(
                {'error': 'Organization not found'}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        Organization.delete(org_id)
        return Response(status=status.HTTP_204_NO_CONTENT)

class IndustryListView(views.APIView):
    """API view for listing industries"""
    
    def get(self, request):
        """Get all industries"""
        industries = Industry.get_all()
        serializer = IndustrySerializer(industries, many=True)
        return Response(serializer.data)

class KeywordListView(views.APIView):
    """API view for listing keywords"""
    
    def get(self, request):
        """Get all keywords"""
        keywords = Keyword.get_all()
        serializer = KeywordSerializer(keywords, many=True)
        return Response(serializer.data)