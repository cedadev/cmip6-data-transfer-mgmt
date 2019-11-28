
import sqlite3
from sqlite3 import Error

class ConnectDB(object):

    def __init__(self, database_file=False, verbose=False):
        self.database_file = database_file
        self.verbose = verbose
        self.conn =  self.connect_to_database()



    def connect_to_database(self):

        if self.verbose:
            print("Connecting to database: {}".format(self.database_file))

        try:
            conn = sqlite3.connect(self.database_file)
            return conn
        except Error as e:
            if verbose:
                print(e)
        return None

    def run_database_query(self, query_str):

        if self.verbose:
            print("Querying database: {}".format(query_str))
        query = self.conn.cursor()
        query.execute(query_str)
        return query.fetchall()

    def query_database(self, query):

        if self.conn:
            return self.run_database_query(query)
        else:
            return "Unable to connect to database"
