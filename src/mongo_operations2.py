import time

from sql_alchemy_operations import SQlAlchemyOperations
from utils.db_connector import DBConnector

from constants import DATABASE


class MongoDBOperations:
    def __init__(self, company_id):
        self.company_id = str(company_id)
        collection_name = "va_{company_id}".format(company_id=self.company_id)

        db_connector = DBConnector()
        self.conn = db_connector.get_remote_mongo_client(section_name='remote_mongo_config')

        database = self.conn[DATABASE]
        self.collection = database[collection_name]

        self.collection.find()

    def insert_raw_data_into_mongo(self):
        data = SQlAlchemyOperations.get_computeinfojson(DATABASE, self.company_id)

        print "Mongo insert started"
        start = time.time()

        while True:
            data_chunk = data.fetchmany(2000)

            if not data_chunk:
                break

            result = [{'rid': row[0],
                       'ex': row[1],
                       'cj': row[2]} for row in data_chunk]

            self.collection.insert_many(result)

        print "Mongo insert complete in {time}".format(time=time.time() - start)

    def delete_rev_dp_id(self):
        print "Delete started"
        start = time.time()

        self.collection.delete_many({
            "rid": {
                "$in": [
                    8515843352, 8515843351
                ]
            }
        })

        print "Mongo fetch completed in {time}".format(time=time.time() - start)

    def get_rev_dp_data(self):
        print "Fetch started"
        start = time.time()

        data = list(self.collection.find({
            "rid": {
                "$in": [
                    8515843352, 8515843351
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
    #
    # obj.get_rev_dp_data()

    # obj.fetch_sql_data()

    # obj.delete_rev_dp_id()
