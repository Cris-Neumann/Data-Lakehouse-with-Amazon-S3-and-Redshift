import pymongo

def donwload_from_MongoDB(db_name:str, collection_name:str) -> list:
    """Download data from MongoDB.
    Args:
        db_name (str): Database name in MongoDB.
        collection_name (str): Collection name in MongoDB.
    Returns:
        list_with_json (list): list of jsons with the MongoDB data.
    """
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]
    list_with_json = list(collection.find({}, {'_id': 0}))
    return list_with_json
