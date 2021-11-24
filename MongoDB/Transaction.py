from Secrets import CONN_STR

from pymongo import MongoClient, ReadPreference, WriteConcern
from pymongo.client_session import ClientSession
from pymongo.collection import Collection, ReturnDocument
from pymongo.database import Database


def increase_accommodates(session: ClientSession, id):
    """
    This method demonstrates how to perform an update
    to a single document's 'accommodates' by 1 with supplied session.
    """
    db: Database = session.client['sample_airbnb']
    collection: Collection = db['listingsAndReviews']

    print('Pre-update document')
    doc = collection.find_one({'_id': id}, {'accommodates': 1})
    print(f"\tAccommodates: {doc['accommodates']}\n")

    print(f"Returned document's value (updated value should be {doc['accommodates'] + 1})")
    # you can choose whether you want to update based on the values from the collection
    # or you can use a pipeline... here we use the collection for simplicity
    #
    # the returned value (in the example below) returns the original (pre-update) document
    doc = collection.find_one_and_update(
        filter={'_id': id},
        update={
                "$inc": {'accommodates': 1}
            }
    )
    print(f"\tAccommodates: {doc['accommodates']}\n")
    print(f"Returned document with ReturnDocument.AFTER (updated value should be {doc['accommodates'] + 2})")
    # to get the updated document
    doc = collection.find_one_and_update(
        filter={'_id': id},
        update={
            "$inc": {'accommodates': 1}
        },
        session=session,
        return_document=ReturnDocument.AFTER
    )
    print(f"\tAccommodates: {doc['accommodates']}")


def edit_bed_type(session: ClientSession, id):
    """
    This method demonstrates how to perform an update
    to a single document's 'bed_type' with supplied session.
    """
    db: Database = session.client['sample_airbnb']
    collection: Collection = db['listingsAndReviews']
    # difference between 'find_one_and_update' and 'update_one'
    # is 'find_one_and_update' lets you sort
    collection.update_one(
        filter={'_id': id},
        update={"$set": {"bed_type": 'Fake!'}},
        session=session
    )


def reset_examples(session):
    """
    Manually 'rolls back' the example commit.
    :param session:
    :return:
    """
    db: Database = session.client['sample_airbnb']
    collection: Collection = db['listingsAndReviews']
    # difference between 'find_one_and_update' and 'update_one'
    # is 'find_one_and_update' lets you sort
    collection.update_one(
        filter={'_id': '10009999'},
        update={"$set": {"bed_type": 'Real Bed'}},
        session=session
    )


if __name__ == '__main__':
    client = MongoClient(CONN_STR)

    # quick single transaction
    with client.start_session() as session:
        # beginning a transaction
        # write concern can be viewed as:
        #   how many server nodes need to copy the transaction for it to be acknowledged?
        #   wtimeout refers to how many ms to wait for propagation
        session.with_transaction(
            callback=lambda s: increase_accommodates(s, '10006546'),
            write_concern=WriteConcern("majority", wtimeout=1000),
            read_preference=ReadPreference.PRIMARY
        )

    print('-' * 100)
    print('A multi-line transaction with commit')
    # in mongodb there is no support for nested transactions
    with client.start_session() as session:
        db: Database = session.client['sample_airbnb']
        collection: Collection = db['listingsAndReviews']
        doc_id = '10009999'

        session.start_transaction(write_concern=WriteConcern("majority"))
        doc = collection.find_one({'_id': doc_id}, {'bed_type': 1})
        print(f'\t  Pre-trans: \tbed_typed="{doc["bed_type"]}"')

        edit_bed_type(session, doc_id)
        doc = collection.find_one({'_id': doc_id}, {'bed_type': 1})
        print(f'\t    Changes: \tbed_type: "{doc["bed_type"]}"')

        session.commit_transaction()
        doc = collection.find_one({'_id': doc_id}, {'bed_type': 1})
        print(f'\tPost-commit: \tbed_typed="{doc["bed_type"]}"')

        print('NOTE: Unlike SQL, you cannot see the changes made until a transaction is committed.\n'
              '\tThat is why it may be helpful to get the original document.')

    print()
    print('-' * 100)
    print('A multi-line transaction with rollback (abort).')
    with client.start_session() as session:
        db: Database = session.client['sample_airbnb']
        collection: Collection = db['listingsAndReviews']
        doc_id = '10266175'

        session.start_transaction(write_concern=WriteConcern("majority"))
        doc = collection.find_one({'_id': doc_id}, {'bed_type': 1})
        print(f'\t Pre-trans: \tbed_typed="{doc["bed_type"]}"')

        session.abort_transaction()
        doc = collection.find_one({'_id': doc_id}, {'bed_type': 1})
        print(f'\tPost-abort: \tbed_typed="{doc["bed_type"]}"')

    # for demonstration reset
    with client.start_session() as session:
        session.with_transaction(callback=reset_examples)