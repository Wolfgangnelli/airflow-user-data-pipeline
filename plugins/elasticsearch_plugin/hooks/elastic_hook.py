from cgitb import reset
from airflow.hooks.base import BaseHook

from elasticsearch import Elasticsearch


# Implement the class corresponding to our hook
class ElasticHook(BaseHook):

    def __init__(self, conn_id='elasticsearch_default', *args, **kwargs):
        # initialize the attributes of base hook
        super().__init__(*args, **kwargs)
        conn = self.get_connection(conn_id) # Now we have the connection and we can get the attributes of that connection

        conn_config = {}
        hosts = [] # elasticsearch can have multiple hosts corresponding to the miltiple machines where elasticsearch is running

        # Attributes of the connection
        if conn.host:
            hosts = conn.host.split(',')
        if conn.port:
            conn_config['port'] = int(conn.port)
        if conn.login:
            conn_config['http_auth'] = (conn.login, conn.password)

        # Create/Initialize the elasticsearch obj, which we are going to use in order to interact with our ElasticSearch instance based on 
        # The attributes of the connection
        self.es = Elasticsearch(hosts, **conn_config)
        # In the attribute schema of the connection from the UI, you will specify the index where you want to store your data
        self.index = conn.schema


    # We can create some additional methods in order to interact with ElasticSearch
    def info(self): # get some info about our elasticsearch instance
        return self.es.info()

    # Define an index
    def set_index(self, index):
        self.index = index

    # Add a document, add data in the index (in elasticsearch instance). attrs: index where the doc will be stored, doc is the data that you want to store
    def add_doc(self, index, doc_type, doc):
        self.set_index(index)
        res = self.es.index(index=index, doc_type=doc_type, doc=doc)
        return res


