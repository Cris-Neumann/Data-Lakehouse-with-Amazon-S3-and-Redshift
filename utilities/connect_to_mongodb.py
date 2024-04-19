import pymongo
import json
from bson import ObjectId

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['yelp_db']
collection = db['raw_business']
data = collection.find()
jsonl_data = '\n'.join([json.dumps({k: str(v) if isinstance(v, ObjectId) else v for k, v in doc.items()}) for doc in data])

# Imprimir los primeros 2 caracteres para verificar
print(jsonl_data[:2])
