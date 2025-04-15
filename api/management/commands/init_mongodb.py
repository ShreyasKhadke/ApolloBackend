"""
Django management command to initialize MongoDB collections
"""
from django.core.management.base import BaseCommand
from django.conf import settings

class Command(BaseCommand):
    help = 'Initialize MongoDB collections for the project'

    def handle(self, *args, **options):
        self.stdout.write('Initializing MongoDB collections...')
        
        db = settings.MONGO_DB
        
        # List of collections to create
        collections = ['organization', 'industry', 'keyword', 'people', 'combinations']
        
        # Get existing collections
        existing_collections = db.list_collection_names()
        
        # Create collections if they don't exist
        for collection_name in collections:
            if collection_name not in existing_collections:
                db.create_collection(collection_name)
                self.stdout.write(self.style.SUCCESS(f'Created collection: {collection_name}'))
            else:
                self.stdout.write(f'Collection already exists: {collection_name}')
        
        # Create indexes for faster queries
        self.stdout.write('Creating indexes...')
        
        # Organization indexes
        db.organization.create_index("apollo_id", unique=True)
        db.organization.create_index("name")
        
        # Industry indexes
        db.industry.create_index("name", unique=True)
        
        # Keyword indexes
        db.keyword.create_index("name", unique=True)
        
        # Combinations indexes (new)
        db.combinations.create_index([("location", 1), ("industry_name", 1)], unique=True)
        db.combinations.create_index("status")
        
        self.stdout.write(self.style.SUCCESS('MongoDB initialization completed successfully'))