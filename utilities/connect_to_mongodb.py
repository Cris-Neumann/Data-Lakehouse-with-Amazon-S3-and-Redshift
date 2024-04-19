import pymongo
import json

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['yelp_db']
collection = db['raw_business']
json_array_data = list(collection.find({}, {'_id': 0}))
json_row_data = '\n'.join([json.dumps(obj) for obj in json_array_data])
