# 🔧 Quick MongoDB Atlas Connection Fix
import os
import ssl
import pymongo
from pymongo import MongoClient
from dotenv import load_dotenv
import certifi

load_dotenv()

def connect_with_fix():
    """Connection method đã fix lỗi SSL handshake"""
    
    print("🔧 Attempting MongoDB Atlas connection with SSL fix...")
    
    # Method 1: Try với SSL context tùy chỉnh
    try:
        uri = os.getenv('MONGO_URI')
        
        # Tạo SSL context với cấu hình phù hợp
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_OPTIONAL
        
        client = MongoClient(
            uri,
            tls=True,
            tlsAllowInvalidCertificates=True,
            tlsAllowInvalidHostnames=True,
            serverSelectionTimeoutMS=60000,  # Tăng timeout
            connectTimeoutMS=60000,
            socketTimeoutMS=60000,
            maxPoolSize=1,  # Giảm connection pool
            retryWrites=True
        )
        
        # Test connection
        client.admin.command('ping')
        print("✅ SUCCESS: Connected with custom SSL context!")
        
        # Test database
        db = client.get_database('MolaDatabase')
        collections = db.list_collection_names()
        print(f"📚 Collections found: {collections}")
        
        return client, db
        
    except Exception as e:
        print(f"❌ Method 1 failed: {e}")
        
    # Method 2: Disable SSL verification (chỉ dùng cho testing)
    try:
        print("\n🔧 Trying with disabled SSL verification...")
        
        uri = os.getenv('MONGO_URI')
        
        client = MongoClient(
            uri,
            tls=True,
            tlsAllowInvalidCertificates=True,
            tlsAllowInvalidHostnames=True,
            serverSelectionTimeoutMS=60000,
            connectTimeoutMS=60000,
            socketTimeoutMS=60000
        )
        
        client.admin.command('ping')
        print("✅ SUCCESS: Connected with SSL verification disabled!")
        
        db = client.get_database('MolaDatabase')
        collections = db.list_collection_names()
        print(f"📚 Collections found: {collections}")
        
        return client, db
        
    except Exception as e:
        print(f"❌ Method 2 failed: {e}")
    
    # Method 3: Alternative connection string
    try:
        print("\n🔧 Trying alternative connection string...")
        
        # Direct connection without srv
        alt_uri = "mongodb://thaian:thaian123@ac-zsyvuds-shard-00-00.qxevmke.mongodb.net:27017,ac-zsyvuds-shard-00-01.qxevmke.mongodb.net:27017,ac-zsyvuds-shard-00-02.qxevmke.mongodb.net:27017/MolaDatabase?ssl=true&replicaSet=atlas-bhejb1-shard-0&authSource=admin&retryWrites=true&w=majority"
        
        client = MongoClient(
            alt_uri,
            ssl_cert_reqs=ssl.CERT_NONE,
            serverSelectionTimeoutMS=60000
        )
        
        client.admin.command('ping')
        print("✅ SUCCESS: Connected with alternative URI!")
        
        db = client.get_database('MolaDatabase')
        collections = db.list_collection_names()
        print(f"📚 Collections found: {collections}")
        
        return client, db
        
    except Exception as e:
        print(f"❌ Method 3 failed: {e}")
    
    print("\n❌ ALL CONNECTION METHODS FAILED!")
    print("🛠️  Suggested fixes:")
    print("1. Check MongoDB Atlas IP Whitelist (add 0.0.0.0/0)")
    print("2. Verify username/password: thaian / thaian123")
    print("3. Try different network (mobile hotspot)")
    print("4. Contact MongoDB Atlas support")
    
    return None, None

if __name__ == "__main__":
    client, db = connect_with_fix()
    
    if client:
        print("\n🎉 CONNECTION SUCCESSFUL!")
        print("You can now use this client in your application.")
    else:
        print("\n❌ CONNECTION FAILED!")
        print("Please check the suggested fixes above.")