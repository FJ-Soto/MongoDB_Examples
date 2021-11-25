from Secrets import CONN_STR

from pymongo import ASCENDING
from pymongo import MongoClient


def recreate_dbs():
    # for testing and demonstration, this wipes everything
    client = MongoClient(CONN_STR)

    # create a database
    # NOTE: does not get added until you create a collection
    db = client["MongoDS"]
    print(f"Collections in DB: {', '.join(client.list_database_names()[:3])}...")

    # checking if the collection exists
    if 'Members' in db.list_collection_names():
        db['Members'].drop()
        print('"Members" already exists. Dropped.')

        # creating collection with a jsonSchema
    db.create_collection(
        "Members",
        validator={
            "$jsonSchema": {
                'bsonType': "object",
                'required': ['fname', 'lname', 'email'],
                'properties': {
                    'fname': {
                        'bsonType': 'string',
                        'description': 'Team member first name.'
                    },
                    'lname': {
                        'bsonType': 'string',
                        'description': 'Team member last name.'
                    },
                    'email': {
                        'bsonType': 'string',
                        'pattern': r'^.+@.+\.[a-zA-Z]+$'
                    },
                    'majors': {
                        'bsonType': 'array',
                        'description': "An array containing member's majors",
                        'minItems': 1,
                        'maxItems': 3
                    }
                }
            }
        },
        # validation levels include 'strict', 'moderate', 'off'
        # strict enforces on all inserts and updates
        # moderate only enforces on first insert but not on updates
        validationLevel='strict',
        # validation actions tells what to do when validation is not met
        # 'error' means that an error is thrown and no insert or update is performed
        # 'warn' means that a warning is logged by action performed
        validationAction='error'
    )

    members_col = db['Members']
    members_col.create_index(
        keys=[
            ('email', ASCENDING)
        ],
        name='student_id',
        unique=True,
        min=1
    )
    print('"Members" created.')

    if 'Professors' in db.list_collection_names():
        db['Professors'].drop()
        print('"Professors" already exists. Dropped.')

    db.create_collection(
        'Professors',
        validator={
            '$or':
                [
                    {
                        'fname': {
                            '$type': 'string'
                        },
                        'email': {
                            '$type': 'string'
                        }
                    }
                ]
        }
    )

    prof_col = db['Professors']

    prof_col.create_index(
        keys=[
            ('email', ASCENDING)
        ],
        name='professor_id',
        unique=True,
        min=1
    )
    print('"Professors" created.')

    print(f"Collections in DB: {', '.join(client.list_database_names()[:3])}...")


def drop():
    client = MongoClient(CONN_STR)
    db = client["MongoDS"]

    if 'Members' in db.list_collection_names():
        db['Members'].drop()

    if 'Professors' in db.list_collection_names():
        db['Professors'].drop()


if __name__ == '__main__':
    recreate_dbs()
    # drop()
