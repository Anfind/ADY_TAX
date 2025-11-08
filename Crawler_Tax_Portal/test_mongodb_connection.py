# 🔧 MongoDB Atlas Connection Test & Fix Script
import os
import ssl
import sys
import pymongo
from pymongo import MongoClient
from dotenv import load_dotenv
import certifi

print("🔍 MONGODB ATLAS CONNECTION TROUBLESHOOTING")
print("=" * 60)

# Load environment variables
load_dotenv()

def test_connection_method_1():
    """Method 1: Standard connection với SSL verification"""
    print("\n🧪 Test 1: Standard SSL Connection")
    try:
        uri = os.getenv('MONGO_URI')
        print(f"URI: {uri[:50]}...")
        
        client = MongoClient(uri, serverSelectionTimeoutMS=30000)
        client.admin.command('ping')
        print("✅ Test 1: SUCCESS - Standard connection works!")
        return client
    except Exception as e:
        print(f"❌ Test 1: FAILED - {str(e)[:100]}...")
        return None

def test_connection_method_2():
    """Method 2: Connection với SSL disabled (không khuyến khích production)"""
    print("\n🧪 Test 2: No SSL Verification (insecure)")
    try:
        uri = os.getenv('MONGO_URI')
        
        client = MongoClient(
            uri,
            ssl=True,
            ssl_cert_reqs=ssl.CERT_NONE,
            serverSelectionTimeoutMS=30000
        )
        client.admin.command('ping')
        print("✅ Test 2: SUCCESS - No SSL verification works!")
        return client
    except Exception as e:
        print(f"❌ Test 2: FAILED - {str(e)[:100]}...")
        return None

def test_connection_method_3():
    """Method 3: Connection với custom SSL context"""
    print("\n🧪 Test 3: Custom SSL Context")
    try:
        uri = os.getenv('MONGO_URI')
        
        # Custom SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        client = MongoClient(
            uri,
            ssl_context=ssl_context,
            serverSelectionTimeoutMS=30000
        )
        client.admin.command('ping')
        print("✅ Test 3: SUCCESS - Custom SSL context works!")
        return client
    except Exception as e:
        print(f"❌ Test 3: FAILED - {str(e)[:100]}...")
        return None

def test_connection_method_4():
    """Method 4: Connection với certifi certificates"""
    print("\n🧪 Test 4: Certifi Certificate Bundle")
    try:
        uri = os.getenv('MONGO_URI')
        
        client = MongoClient(
            uri,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=30000
        )
        client.admin.command('ping')
        print("✅ Test 4: SUCCESS - Certifi certificates work!")
        return client
    except Exception as e:
        print(f"❌ Test 4: FAILED - {str(e)[:100]}...")
        return None

def test_connection_method_5():
    """Method 5: Manual connection string construction"""
    print("\n🧪 Test 5: Manual Connection String")
    try:
        # Manual connection string
        username = "thaian"
        password = "thaian123"
        cluster = "taxanalyses.qxevmke.mongodb.net"
        
        uri = f"mongodb+srv://{username}:{password}@{cluster}/MolaDatabase?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE"
        
        client = MongoClient(uri, serverSelectionTimeoutMS=30000)
        client.admin.command('ping')
        print("✅ Test 5: SUCCESS - Manual connection works!")
        return client
    except Exception as e:
        print(f"❌ Test 5: FAILED - {str(e)[:100]}...")
        return None

def check_environment():
    """Check system environment"""
    print("\n🔧 SYSTEM ENVIRONMENT CHECK")
    print(f"Python version: {sys.version}")
    print(f"PyMongo version: {pymongo.version}")
    print(f"SSL version: {ssl.OPENSSL_VERSION}")
    print(f"Certifi location: {certifi.where()}")
    
    # Check if running in virtual environment
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("🐍 Running in virtual environment")
    else:
        print("🐍 Running in system Python")

def fix_suggestions():
    """Provide fix suggestions"""
    print("\n🛠️  SUGGESTED FIXES:")
    print("1. Update PyMongo: pip install --upgrade pymongo")
    print("2. Update Certifi: pip install --upgrade certifi")
    print("3. Install required SSL packages: pip install pyopenssl")
    print("4. Check MongoDB Atlas IP Whitelist (add 0.0.0.0/0 for testing)")
    print("5. Try different network (mobile hotspot, VPN)")
    print("6. Check Windows Firewall settings")
    
def run_all_tests():
    """Run all connection tests"""
    check_environment()
    
    working_client = None
    methods = [
        test_connection_method_1,
        test_connection_method_2, 
        test_connection_method_3,
        test_connection_method_4,
        test_connection_method_5
    ]
    
    for method in methods:
        client = method()
        if client and not working_client:
            working_client = client
            break
    
    if working_client:
        print(f"\n🎉 FOUND WORKING CONNECTION!")
        
        # Test database operations
        try:
            db = working_client.get_database('MolaDatabase')
            collections = db.list_collection_names()
            print(f"📚 Available collections: {collections}")
            
            # Test reading data
            if collections:
                first_collection = collections[0]
                sample_doc = db[first_collection].find_one()
                print(f"📄 Sample from {first_collection}: {str(sample_doc)[:100]}...")
                
        except Exception as e:
            print(f"⚠️  Database operations failed: {e}")
        
        return working_client
    else:
        print(f"\n❌ ALL CONNECTION METHODS FAILED!")
        fix_suggestions()
        return None

if __name__ == "__main__":
    working_client = run_all_tests()
    
    if working_client:
        print("\n✅ Connection successful! You can use this client for your application.")
    else:
        print("\n❌ Connection failed. Please try the suggested fixes.")