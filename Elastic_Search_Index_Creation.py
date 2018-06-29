#####################################
####Elastic search implementation####
#####################################

##importing required libraries
import requests
import os
from sqlalchemy import create_engine
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
#from tkinter import *
#import tkinter

##Setting up the working directory
os.chdir("K:\HE Project - FAMA\Elastic Search Code")

##Checking whether elastic server is started or not
res = requests.get('http://localhost:9200')
print(res.content)

##Connection to ElasticSearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

##Creating a index, index here refers a database in our language
es.indices.create(index='helloworld',body={})

##Connecting to MySQL for fetching the data
engine = create_engine('mysql+mysqlconnector://root:Creative@372@localhost:3306/ghc_fama', echo=False)
cnx = engine.connect()
data = pd.read_sql('SELECT * FROM ghc_fama.fama_reports', cnx)

##Bulk inserting documents. Each row in the DataFrame will be a document in ElasticSearch
documents = data.to_dict(orient='records')
bulk(es, documents, index='helloworld',doc_type='foo', raise_on_error=True)

