import time

from sqlalchemy.sql import *

from constants import DATABASE
from models import get_sli_revision_model
from utils.db_connector import DBConnector


class SQlAlchemyOperations:
    db_connector_obj = DBConnector()

    def __init__(self):
        pass

    @classmethod
    def get_data_from_raw_query(cls, db, company_id):
        print "Started fetching data from MySQL company id: {company_id}".format(company_id=company_id)

        start = time.time()

        query = "SELECT revisiondpid, expression FROM {db}.`{company_id}`".format(db=db, company_id=company_id)
        conn = cls.db_connector_obj.get_mysql_engine(db=db)

        data = conn.execute(text(query))

        print "Completed fetching data for {db} in {time}".format(db=db, time=time.time() - start)

        return data

    @classmethod
    def get_computeinfojson(cls, db, company_id):
        print "Started fetching data from MySQL company id: {company_id}".format(company_id=company_id)

        start = time.time()
        query = "SELECT revisiondpid, expression, computeinfojson FROM {db}.{company_id}".format(db=db,
                                                                                                 company_id=company_id)

        conn = cls.db_connector_obj.get_mysql_engine(db=db)

        data = conn.execute(text(query))

        print "Completed fetching data for {db} in {time}".format(db=db, time=time.time() - start)
        return data

    @classmethod
    def get_model(cls, company_id, db):
        conn = cls.db_connector_obj.get_mysql_engine(db=db)
        session = cls.db_connector_obj.get_sql_alchemy_session(conn)
        return get_sli_revision_model(company_id, session)

    @classmethod
    def get_local_mysql_data(cls, db, company_id):
        print "Started fetching data from MySQL company id: {company_id}".format(company_id=company_id)

        start = time.time()
        query = "SELECT revisiondpid, expression, computeinfojson FROM {db}.`{company_id}` " \
                "WHERE revisiondpid IN (9529232226, 8117414530, 8117415374)".format(db=db, company_id=company_id)

        conn = cls.db_connector_obj.get_mysql_engine(db=db)
        data = list(conn.execute(text(query)))

        print "Completed fetching data for {db} in {time}".format(db=db, time=time.time() - start)

        return list(data)

    @classmethod
    def insert_bulk_data_result(cls, db, model, rows, chunk_size=1000):
        """
        Insert one table output into another table
        :param db: Db name of the config file
        :param model: Model of the table in which data will be inserted
        :param rows: Data which need to be inserted
        :param chunk_size: Chunk size by which data will be inserted
        """

        start = time.time()
        engine = cls.db_connector_obj.get_mysql_engine(db=db)

        with engine.connect() as conn:
            with conn.begin() as trans:
                try:
                    while True:
                        data_chunk = rows.fetchmany(chunk_size)

                        if not data_chunk:
                            break

                        conn.execute(model.__table__.insert(), *rows)

                    trans.commit()
                except Exception as ex:
                    trans.rollback()
                    print "An error occurred"
                    raise

        print "Completed data insertion in {time}".format(time=time.time() - start)


if __name__ == '__main__':
    model_obj = SQlAlchemyOperations.get_model(13059, 'mysql_text')
    data = SQlAlchemyOperations.get_computeinfojson(Database, 13059)

    SQlAlchemyOperations.insert_bulk_data_result('mysql_text', model_obj, data)
    SQlAlchemyOperations.get_local_mysql_data('mysql_text', 13059)

    print "Hello"
