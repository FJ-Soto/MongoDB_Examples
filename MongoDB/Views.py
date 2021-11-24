from Secrets import CONN_STR

from pprint import pprint
from pymongo import MongoClient
from pymongo.cursor import Cursor
from pymongo.database import Database
from pymongo.collection import Collection


if __name__ == '__main__':
    client = MongoClient(CONN_STR)
    db: Database = client['sample_airbnb']
    collection: Collection = db['listingsAndReviews']

    if 'review_view' in db.list_collection_names():
        db.drop_collection('review_view')
        print('"review_view" already existed. Dropped "review_view".')

    res = db.create_collection(
        name='review_view',
        viewOn='listingsAndReviews',
        pipeline=[
            {'$project': {'reviews': 1}}
        ]
    )
    # in compass, this will have a different icon

    # select three documents and display at most 2 reviews
    cursor: Cursor = res.find(
        {},
        {'_id': 1,
         'reviews': {'$slice': ["$reviews", 2]}}
    ).limit(3)

    for doc in cursor:
        print('-' * 100)
        pprint(doc)
