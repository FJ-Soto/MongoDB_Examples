from Secrets import CONN_STR

from pprint import pprint
from pymongo import MongoClient


if __name__ == '__main__':
    client = MongoClient(CONN_STR)

    db = client['MongoDS']

    # collection = db['Professors']
    res = db.Professors.aggregate([
        {
            '$lookup':
                {
                    'from': 'Members',
                    'localField': 'email',
                    'foreignField': 'professor_id',
                    'as': "advises"
                }
        }
    ])

    print('The result is similar to having one like')
    print('-'*100)
    print('SELECT *\n'
          'FROM\n'
          '\tProfessors P LEFT OUTER JOIN Members M\n'
          '\tON P.email = M.professor_id\n')
    print('-'*100)

    for doc in res:
        pprint(doc)
        print()
