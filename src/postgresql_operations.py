import time

from psycopg2.extras import execute_values

from constants import Database
from sql_alchemy_operations import SQlAlchemyOperations
from utils.db_connector import DBConnector


class PostgreSQL:
    def __init__(self, company_id):
        self.company_id = str(company_id)
        self.db_connector = DBConnector()
        self.conn = self.db_connector.get_postgre_conn("postgres_sql")

    def get_mysql_data(self):
        return SQlAlchemyOperations.get_computeinfojson(Database, self.company_id)

    def insert(self, rows, db):
        try:
            print "Insertion started"

            start = time.time()
            self.conn.autocommit = False
            cursor = self.conn.cursor()
            query = 'INSERT INTO "{table}" (revisiondpid, expression, computeinfojson) VALUES %s'
            query = query.format(db=db, table=self.company_id)

            while True:
                data_chunk = rows.fetchmany(1000)
                if not data_chunk:
                    break

                execute_values(cursor, query, tuple(data_chunk))

            self.conn.commit()

            print "All rows inserted in {time}".format(time=time.time() - start)

        except Exception as ex:
            print "An error occurred"
            self.conn.rollback()

        finally:
            self.conn.autocommit = True
            cursor.close()

    def get_postgre_data(self, db):
        print "Started fetching data from MySQL company id: {company_id}".format(company_id=self.company_id)

        start = time.time()
        cursor = self.conn.cursor()

        query = 'SELECT revisiondpid, expression, computeinfojson FROM "{company_id}" ' \
                'WHERE revisiondpid IN (9529232226, 8117414530, 8117415374)'.format(company_id=self.company_id)

        cursor.execute(query)
        data = cursor.fetchall()

        print "Completed fetching data for {db} in {time}".format(db=db, time=time.time() - start)

        return list(data)


if __name__ == '__main__':
    postgre_obj = PostgreSQL(13059)
    data = postgre_obj.get_mysql_data()

    postgre_obj.insert(data, Database)
    postgre_obj.get_postgre_data(Database)
