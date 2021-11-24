# now, now, this isn't about love...
# JK this is about our LOVE for MongoDB!
from pymongo import MongoClient
from pprint import pprint

from Secrets import CONN_STR

if __name__ == '__main__':
    client = MongoClient(CONN_STR)

    db = client['MongoDS']

    collection = db['Members']
    res = collection.aggregate([
        {
            '$lookup':
                {
                    'from': 'Professors',
                    'localField': 'professor_id',
                    'foreignField': 'email',
                    'as': "professor"
                }
        }
    ])

    print('The result is similar to having one like:\n')
    print('\tSELECT *\n'
          '\tFROM\n'
          '\t\tMembers M INNER JOIN Professors P\n'
          '\t\tON M.professor_id = P.email\n')
    for doc in res:
        pprint(doc)
