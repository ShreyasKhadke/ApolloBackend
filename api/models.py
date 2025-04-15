"""
Models for API app - these are not traditional Django ORM models
but helper classes to work with MongoDB collections
"""
from django.conf import settings
from bson import ObjectId

class Organization:
    """Helper class for organization data structure"""
    collection = settings.MONGO_DB['organization']
    
    @classmethod
    def get_all(cls, limit=None, skip=None):
        """Get all organizations with optional pagination"""
        query = cls.collection.find()
        
        if skip:
            query = query.skip(skip)
        if limit:
            query = query.limit(limit)
            
        return list(query)
    
    @classmethod
    def get_by_id(cls, org_id):
        """Get organization by MongoDB id"""
        try:
            return cls.collection.find_one({"_id": ObjectId(org_id)})
        except:
            return None
            
    @classmethod
    def get_by_apollo_id(cls, apollo_id):
        """Get organization by Apollo ID"""
        return cls.collection.find_one({"apollo_id": apollo_id})
    
    @classmethod
    def create(cls, org_data):
        """Create new organization"""
        result = cls.collection.insert_one(org_data)
        return result.inserted_id
    
    @classmethod
    def update(cls, org_id, org_data):
        """Update organization by ID"""
        return cls.collection.update_one(
            {"_id": ObjectId(org_id)},
            {"$set": org_data}
        )
    
    @classmethod
    def delete(cls, org_id):
        """Delete organization by ID"""
        return cls.collection.delete_one({"_id": ObjectId(org_id)})

class Industry:
    """Helper class for industry data structure"""
    collection = settings.MONGO_DB['industry']
    
    @classmethod
    def get_all(cls):
        """Get all industries"""
        return list(cls.collection.find())
    
    @classmethod
    def get_by_name(cls, name):
        """Get industry by name"""
        return cls.collection.find_one({"name": name})
    
    @classmethod
    def create(cls, name):
        """Create new industry"""
        if not cls.get_by_name(name):
            result = cls.collection.insert_one({
                "name": name,
                "created_at": None,
                "updated_at": None
            })
            return result.inserted_id
        return None

class Keyword:
    """Helper class for keyword data structure"""
    collection = settings.MONGO_DB['keyword']
    
    @classmethod
    def get_all(cls):
        """Get all keywords"""
        return list(cls.collection.find())
    
    @classmethod
    def get_by_name(cls, name):
        """Get keyword by name"""
        return cls.collection.find_one({"name": name})
    
    @classmethod
    def create(cls, name):
        """Create new keyword"""
        if not cls.get_by_name(name):
            result = cls.collection.insert_one({
                "name": name,
                "created_at": None,
                "updated_at": None
            })
            return result.inserted_id
        return None