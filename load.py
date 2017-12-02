import os
from elasticsearch import Elasticsearch

es = Elasticsearch([os.environ['ELASTIC_URL']], verify_certs=True)

if not es.ping():
    raise ValueError("Connection failed")
else:
    print('Successfully connected to: ' + os.environ['ELASTIC_URL'])
