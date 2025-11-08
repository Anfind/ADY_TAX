from pymongo import MongoClient

try:
    client = MongoClient("mongodb+srv://thaian:thaian123@taxanalyses.qxevmke.mongodb.net/?retryWrites=true&w=majority&appName=TaxAnalyses")
    client.admin.command('ping')
    print("✅ Connected to TaxAnalyses cluster successfully!")
    
    # Test database access
    db = client.MolaDatabase
    print(f"✅ Access to MolaDatabase: {db.name}")
    
except Exception as e:
    print(f"❌ Connection failed: {e}")