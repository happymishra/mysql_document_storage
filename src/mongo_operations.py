import time

import pymongo

from sql_alchemy_operations import SQlAlchemyOperations
from utils.db_connector import DBConnector


class MongoDBOperations:
    def __init__(self, company_id):
        self.company_id = str(company_id)
        db_connector = DBConnector()
        sli_rev_db = db_connector.get_mongo_client(db='mongo_dest')['slirevision']

        self.collection = sli_rev_db[self.company_id]
        self.collection.create_index([("revisiondpid", pymongo.ASCENDING)])

    def insert_raw_data_into_mongo(self):
        print "Mongo insert started"
        start = time.time()

        data = SQlAlchemyOperations.get_data_from_raw_query('slirevision', self.company_id)

        while True:
            data_chunk = data.fetchmany(1000)

            if not data_chunk:
                break

            result = [dict(row) for row in data_chunk]
            self.collection.insert_many(result)

        print "Mongo insert complete in {time}".format(time=time.time() - start)

    def get_rev_dp_data(self):
        print "Fetch started"
        start = time.time()

        data = list(self.collection.find({
            "revisiondpid": {
                "$in": [
                    8544784405, 9945967888, 9945967584
                ]
            }
        }))

        print "Mongo fetch completed in {time}".format(time=time.time() - start)

    def get_full_data(self):
        data = list(self.collection.find())

    def filter_data(self):
        data = list(self.collection.find({'periodstart': "FY-2013"}))


if __name__ == '__main__':
    obj = MongoDBOperations(13059)
    obj.insert_raw_data_into_mongo()

    obj.get_rev_dp_data()
