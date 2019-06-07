import time

import pymongo

from constants import Database
from sql_alchemy_operations import SQlAlchemyOperations
from utils.db_connector import DBConnector


class MongoDBOperations:
    def __init__(self, company_id):
        self.company_id = str(company_id)
        db_connector = DBConnector()
        sli_rev_db = db_connector.get_mongo_client(db='mongo_dest')[Database]

        self.collection = sli_rev_db[self.company_id]
        self.collection.create_index([("revisiondpid", pymongo.ASCENDING)])

    def insert_raw_data_into_mongo(self):
        data = SQlAlchemyOperations.get_computeinfojson(Database, self.company_id)

        print "Mongo insert started"
        start = time.time()

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
                    9529232226, 8117414530, 8117415374
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
