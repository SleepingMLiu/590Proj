from pymongo import MongoClient
import datetime

MONGO_URI = "mongodb+srv://emliu:Xiaoliu254@de-cluster.oa2jc.mongodb.net/?retryWrites=true&w=majority&appName=de-cluster"

DB_NAME = "wildfire_pipeline"
COLLECTION_NAME = "user_logs"

def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return collection

def log_user_action(user_id, action, details, status="submitted"):
    collection = get_mongo_collection()
    log_entry = {
        "timestamp": datetime.datetime.utcnow(),
        "user_id": user_id,
        "action": action,
        "details": details,
        "status": status
    }
    collection.insert_one(log_entry)
    print(f"Logged user action to MongoDB: {action} for {user_id}")
