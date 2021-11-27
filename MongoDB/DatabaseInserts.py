from Secrets import CONN_STR
from DatabaseConstraints import recreate_dbs

from pymongo import MongoClient
from pymongo.errors import WriteError
from pymongo.collection import Collection
from pymongo.results import InsertManyResult


if __name__ == '__main__':
    client = MongoClient(CONN_STR)

    db = client['MongoDS']

    # sadly, validation errors are not helpful
    # the document below does not insert and yields 'Document failed validation...'
    # the reason for it is that size not in (1...4)
    collection = db['Members']
    try:
        collection.insert_one(
            {
                'fname': 'Fernando',
                'lname': 'Soto',
                'email': 'fsoto@my.dom.edu',
                'majors': []
            },
        )
    except WriteError as err:
        print(err)
    finally:
        collection.insert_one(
            {
                'fname': 'Fernando',
                'lname': 'Soto',
                'email': 'fsoto@my.dom.edu',
                'majors': ['Computer Science'],
            },
            # you can specify whether to bypass validation...
            bypass_document_validation=False
        )

    collection = db['Professors']
    try:
        # note that you can add fields that are not defined in the schema
        # this a blessing and a curse... assigning the wrong field name can be problematic
        res: InsertManyResult = collection.insert_many(
            documents=[
                {
                    'fname': 'Cyrus',
                    'lname': 'Grant',
                    'email': 'grantcn@dom.edu',
                    'department': 'Computer Science'
                },
                {
                    'fname': 'Mark',
                    'lname': 'Hodges',
                    'email': 'mhodges@dom.edu'
                }
            ]
        )

        mems: Collection = db['Members']
        mems.find_one_and_update(
            filter={'email': 'fsoto@my.dom.edu'},
            update={
                '$set': {
                    'professor_id': 'mhodges@dom.edu'
                }
            }
        )
    except Exception as err:
        print("P", err)

    # NOTES: Many developers defend the weird actions of MongoDB by claiming that
    # this is expected from a flexible, schemaless data model. Additionally,
    # it is argued that the validations and proper rigorous checks should be performed
    # at the business logic layer. The DB validation should come second.
