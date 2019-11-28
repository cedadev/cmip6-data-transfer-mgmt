#!/usr/bin/env Python

import requests
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

class QuerySolr(object):

    """
    Directly query the solr shards without using Tomcat for faster and more reliable searching.
    """

    def __init__(self,
                 url='https://esgf-index1.ceda.ac.uk/esg-search/search/',
                 url_ds='https://esgf-index1.ceda.ac.uk/solr/datasets/select?q=*:*&shards='
                        'esgf-index2.ceda.ac.uk:8996/solr/datasets|esgf-data.dkrz.de/solr/datasets,'
                        'esgf-index2.ceda.ac.uk:8998/solr/datasets|esgf-node.llnl.gov/solr/datasets,'
                        'esgf-index2.ceda.ac.uk:8999/solr/datasets|esg-dn1.nsc.liu.se/solr/datasets,'
                        'esgf-index2.ceda.ac.uk:9000/solr/datasets|esgf-node.ipsl.upmc.fr/solr/datasets,'
                        'esgf-index2.ceda.ac.uk:9001/solr/datasets|esgdata.gfdl.noaa.gov/solr/datasets,'
                        'esgf-index2.ceda.ac.uk:9003/solr/datasets|esgf.nccs.nasa.gov/solr/datasets,'
                        'esgf-index2.ceda.ac.uk:9004/solr/datasets|esgf.nci.org.au/solr/datasets,'
                        'esgf-index2.ceda.ac.uk:9005/solr/datasets|esg.pik-potsdam.de/solr/datasets,'
                        'localhost:8983/solr/datasets,'
                        'esgf-index3.ceda.ac.uk:8983/solr/datasets,'
                        'esgf-index4.ceda.ac.uk:8983/solr/datasets',
                 url_file='https://esgf-index1.ceda.ac.uk/solr/files/select?q=*:*&shards='
                          'esgf-index2.ceda.ac.uk:8996/solr/files|esgf-data.dkrz.de/solr/files,'
                          'esgf-index2.ceda.ac.uk:8998/solr/files|esgf-node.llnl.gov/solr/files,'
                          'esgf-index2.ceda.ac.uk:8999/solr/files|esg-dn1.nsc.liu.se/solr/files,'
                          'esgf-index2.ceda.ac.uk:9000/solr/files|esgf-node.ipsl.upmc.fr/solr/files,'
                          'esgf-index2.ceda.ac.uk:9001/solr/files|esgdata.gfdl.noaa.gov/solr/files,'
                          'esgf-index2.ceda.ac.uk:9003/solr/files|esgf.nccs.nasa.gov/solr/files,'
                          'esgf-index2.ceda.ac.uk:9004/solr/files|esgf.nci.org.au/solr/files,'
                          'esgf-index2.ceda.ac.uk:9005/solr/files|esg.pik-potsdam.de/solr/files,'
                          'localhost:8983/solr/files,'
                          'esgf-index3.ceda.ac.uk:8983/solr/files,'
                          'esgf-index4.ceda.ac.uk:8983/solr/files',
                 verbose=False):
        self.url = url
        self.url_ds = url_ds
        self.url_file = url_file
        self.verbose = verbose



    def query_solr(self, id, query=None, type=None, return_fields=None):

        """
        Query Solr

        :param id: the value to query, e.g. instance_id
        :param query: the field to query against e.g. "instance_id"
        :param type: File or Dataset
        :param return_fields: What parameters are to be returned.
        :return: dictionary of return_fields
        """

        if self.verbose:
            print("querying for {}={}".format(query, id))

        field = return_fields

        if type == 'search':
            params = {'type': type,
                      'limit': '10000',
                      'format': 'application/solr+json',
                      'fields': field}
            params[query] = id

        if type == 'Dataset' or type == 'File':
            params = {'indent': 'true',
                      'rows': '10000',
                      'facet': 'true',
                      'wt': 'json',
                      'fl': field}
            params['fq'] = ['type:{}'.format(type),'{}:{}'.format(query, id)]

        if type == 'search':
            resp = requests.get(self.url, params=params)

        if type == 'Dataset':
            resp = requests.get(self.url_ds, params=params)

        if type == 'File':
            resp = requests.get(self.url_file, params=params)

        if self.verbose:
            print(resp.request.url)

        content = resp.json()
        return content["response"]["docs"][:]


