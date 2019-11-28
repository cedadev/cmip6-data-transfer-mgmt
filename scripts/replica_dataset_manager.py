

import os
import sys
import query_solr
import query_sdt
import argparse


GWS_CACHE = "/group_workspaces/jasmin2/esgf_repl/synda-cmip6/data/"
DATABASE = "/gws/nopw/j04/cmip6_prep_vol1/synda/cmip6_sdt_backups/sdt.db.latest"


def remove_from_cache(dataset_list):

    with open(dataset_list) as r:
        datasets = [ line.strip() for line in r ]

    for ds in datasets:
        ds_path = os.path.join(GWS_CACHE, ds.replace('.', '/'))
        if os.path.exists(ds_path):
            print(os.path.exists(ds_path), len(os.listdir(ds_path)), ds_path)
            for file in os.listdir(ds_path):
                fpath = os.path.join(ds_path, file)
                print("REMOVING {}".format(fpath))
                os.remove(fpath)





def is_dataset_complete():
    sdt = query_sdt.ConnectDB(database_file=DATABASE)
    dataset_list = sys.argv[1]
    solr = query_solr.QuerySolr()
    
    with open(dataset_list) as r:
        datasets = [line.strip() for line in r]
    
    for ds in datasets:
        
        res = solr.query_solr(ds, query="instance_id", type="Dataset", return_fields="replica, data_node")
        master = [r for r in res if not r["replica"]]
        esgf_files = solr.query_solr("{}|{}".format(ds, master[0]["data_node"]),
                                     query="dataset_id",
                                     type="File",
                                     return_fields="title, checksum")
        set_esgf_files = set([f['title'] for f in esgf_files])
        num_master_files = len(set_esgf_files)
    
        cache_dir = os.path.join(GWS_CACHE, ds.replace('.', '/'))
        if not os.path.exists(cache_dir):
            print("NO CACHE DIR {} {}".format(ds, cache_dir))
            continue
        else:
            set_cache_files = set(os.listdir(cache_dir))
            if not len(set_esgf_files) == len(set_cache_files):
                continue
            else:
                if set_cache_files == set_esgf_files:
                    file_checksums_match = True
                    for file in set_cache_files:
                        solr_checksum = [f['checksum'][0] for f in esgf_files if f['title'] == file][0]
                        res = sdt.run_database_query(
                            "SELECT checksum FROM file WHERE file_functional_id='{}.{}'".format(ds, file))
                        synda_checksum = res[0][0]
                        if not solr_checksum == synda_checksum:
                            file_checksums_match = False
    
                    if file_checksums_match:
                        print(ds)


def do_parsing():

    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true", help="increase output verbosity")
    parser.add_argument("-f", "--datasets_file", type=str, help="Filename of list of datasets to process")
    parser.add_argument("-c", "--is_dataset_complete", action="store_true", help="Test if a dataset is complete")
    parser.add_argument("-r", "--remove_from_cache", action="store_true", help="Remove the dataset from the cache")

    args = parser.parse_args()
    if not args.datasets_file:
        parser.error("No filename supplied.")

    return args

if __name__ == "__main__":

    args = do_parsing()
    print(args.datasets_file)
    print(args.is_dataset_complete)
    print(args.remove_from_cache)
