from sqlalchemy.sql import *

from utils.db_connector import DBConnector


class SQlAlchemyOperations:
    db_connector_obj = DBConnector()

    def __init__(self):
        pass

    @classmethod
    def get_data_from_raw_query(cls, db, company_id):
        print "Started fetching data from MySQL company id: {company_id}".format(company_id=company_id)

        # query = "SELECT CAST(revisiondpid AS SIGNED) AS revisiondpid, expression FROM {db}.`{company_id}`
        # LIMIT 1".format(db=db, company_id=company_id)

        query = "SELECT revisiondpid, expression FROM {db}.`{company_id}` LIMIT 1".format(db=db, company_id=company_id)
        conn = cls.db_connector_obj.get_mysql_engine(db=db)

        data = conn.execute(text(query))

        print "Completed fetching data from MySQL company id: {company_id}".format(company_id=company_id)

        return data

    @classmethod
    def insert_bulk_data_result(cls, db, model, rows, chunk_size=1000):
        """
        Insert one table output into another table
        :param db: Db name of the config file
        :param model: Model of the table in which data will be inserted
        :param rows: Data which need to be inserted
        :param chunk_size: Chunk size by which data will be inserted
        """

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
