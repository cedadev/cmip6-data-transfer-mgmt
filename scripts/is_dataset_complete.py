
import os, sys
import query_solr
import sqlite3
from sqlite3 import Error

CACHE = "/group_workspaces/jasmin2/esgf_repl/synda-cmip6/data/"
DATABASE = "/gws/nopw/j04/cmip6_prep_vol1/synda/cmip6_sdt_backups/sdt.db.latest"


class ConnectDB(object):

    def __init__(self, database_file=DATABASE, verbose=False):
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

sdt = ConnectDB()
dataset_list = sys.argv[1]
solr = query_solr.QuerySolr()


with open(dataset_list) as r:
    datasets = [ line.strip() for line in r ]

for ds in datasets:

    res = solr.query_solr(ds, query="instance_id", type="Dataset", return_fields="replica, data_node")
    master = [ r for r in res if not r["replica"] ]
    esgf_files = solr.query_solr("{}|{}".format(ds, master[0]["data_node"]),
                                 query="dataset_id",
                                 type="File",
                                 return_fields="title, checksum")
    set_esgf_files = set([ f['title'] for f in esgf_files ])
    num_master_files = len(set_esgf_files)

    cache_dir = os.path.join(CACHE, ds.replace('.', '/'))
    if not os.path.exists(cache_dir):
        print("NO CACHE DIR {} {}".format(ds,cache_dir))
        continue
    else:
        set_cache_files = set(os.listdir(cache_dir))
        if not len(set_esgf_files) == len(set_cache_files):
            continue
        else:
            if set_cache_files == set_esgf_files:
                file_checksums_match = True
                for file in set_cache_files:
                    solr_checksum = [ f['checksum'][0] for f in esgf_files if f['title'] == file ][0]
                    res = sdt.run_database_query("SELECT checksum FROM file WHERE file_functional_id='{}.{}'".format(ds, file))
                    synda_checksum = res[0][0]
                    if not solr_checksum == synda_checksum:
                        file_checksums_match = False

                if file_checksums_match:
                    print(ds)
