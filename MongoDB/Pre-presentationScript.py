from Secrets import CONN_STR

from pymongo import MongoClient


client = MongoClient(CONN_STR)
db = client['MongoDS']

if 'Professors' in db.list_collection_names():
    db['Professors'].drop()
    print('Dropped Professors')

if 'Members' in db.list_collection_names():
    db['Members'].drop()
    print('Dropped Members')

db = client['sample_airbnb']
if 'review_view' in db.list_collection_names():
    db['review_view'].drop()
    print('Dropped review_view')
