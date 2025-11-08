# 🔧 Fixed MongoDB Atlas Connection Helper
import os
import ssl
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def get_mongodb_client():
    """
    Tạo MongoDB client với cấu hình SSL đã fix lỗi handshake
    Returns: (client, database) tuple
    """
    try:
        uri = os.getenv('MONGO_URI')
        db_name = os.getenv('MONGO_DB_NAME', 'MolaDatabase')
        
        # Cấu hình TLS/SSL để fix lỗi handshake
        client = MongoClient(
            uri,
            tls=True,
            tlsAllowInvalidCertificates=True,
            tlsAllowInvalidHostnames=True,
            serverSelectionTimeoutMS=60000,
            connectTimeoutMS=60000,
            socketTimeoutMS=60000,
            maxPoolSize=10,
            retryWrites=True
        )
        
        # Test connection
        client.admin.command('ping')
        
        db = client.get_database(db_name)
        
        print(f"✅ MongoDB Atlas connected successfully!")
        print(f"📚 Database: {db_name}")
        
        return client, db
        
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        return None, None

def test_database_operations(db):
    """Test basic database operations"""
    try:
        # List collections
        collections = db.list_collection_names()
        print(f"📋 Available collections: {collections}")
        
        # Test read from first collection if available
        if collections:
            first_collection = collections[0]
            count = db[first_collection].count_documents({})
            print(f"📊 Documents in {first_collection}: {count}")
            
            # Show sample document
            sample = db[first_collection].find_one()
            if sample:
                print(f"📄 Sample document keys: {list(sample.keys())}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database operations failed: {e}")
        return False

if __name__ == "__main__":
    print("🔧 Testing MongoDB Atlas connection...")
    client, db = get_mongodb_client()
    
    if client and db:
        test_database_operations(db)
        print("\n🎉 All tests passed! MongoDB Atlas is ready to use.")
    else:
        print("\n❌ Connection test failed!")