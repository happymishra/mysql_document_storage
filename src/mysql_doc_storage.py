import time

from sql_alchemy_operations import SQlAlchemyOperations
from utils.db_connector import DBConnector


class MySQLDocumentStore:
    def __init__(self, company_id):
        self.company_id = str(company_id)

        db_connector = DBConnector()
        self.session = db_connector.get_mysql_ds_session(db='mysql_ds')

        self.db = self.session.get_schema('mysql_ds')
        self.collection = self.db.get_collection(self.company_id)

        if not self.collection.exists_in_database():
            self.collection = self.db.create_collection(self.company_id)

    def populate_mysql_ds(self, rows):
        print "Started populating MySQL Document Storage"
        start = time.time()

        try:
            self.session.start_transaction()

            while True:
                data_chunk = rows.fetchmany(1000)
                if not data_chunk:
                    break

                result = [{'revisiondpid': int(row[0]), 'expression': row[1]} for row in data_chunk]
                self.collection.add(result).execute()

            self.session.commit()

        except Exception as ex:
            print "An error occurred while inserting"

        print "Completed inserted in {seconds}".format(seconds=time.time() - start)

    def get_mysql_data(self):
        return SQlAlchemyOperations.get_data_from_raw_query('slirevision', self.company_id)

    def get_mysql_ds_data(self):
        print "Started fetching data"
        start = time.time()

        result = self.collection.find("revisiondpid IN (8544784405, 9945967888, 9945967584)").execute()
        result = result.fetch_all()

        print "Fetch completed in {time}".format(time=time.time() - start)


if __name__ == '__main__':
    mysql_ds_obj = MySQLDocumentStore(13059)

    data = mysql_ds_obj.get_mysql_data()
    mysql_ds_obj.populate_mysql_ds(data)

    mysql_ds_obj.get_mysql_ds_data()
