# -*- coding: utf-8 -*-
"""
Created on Tue May 22 17:05:20 2018

@author: RamaKrishna
"""

##importing required packages
import os
#import json
import mysql.connector
#import numpy as np
import pandas as pd

##Setting up working directory
os.chdir("K:/HE Project - FAMA/Data/Combined")
Files_Available=os.listdir()
charar = pd.DataFrame(Files_Available,columns=['File_Name'])
#charar.File_Name=charar.File_Name.str.replace(".txt","")
charar['Ticker']=""
charar['Year_Quarter']=charar['File_Name'].str[-10:-4]
charar['Report_Type']=charar['File_Name'].str[-13:-11]
charar['Content']=""
charar['JSON_Content']=""
for j in range(0,len(Files_Available)):
    path=Files_Available[j]
    f=open(path, "r")
    contents =f.read()
    charar.loc[j,['Content']]=contents
    charar.loc[j,['Ticker']]=str(Files_Available[j])[0:str(Files_Available[j]).find("_")]
    #charar.loc[j,['JSON_Content']]=json.dumps(contents)
  
#charar.to_sql('fama_reports',engine, if_exists='append',index=True)
#from sqlalchemy import create_engine
#engine = create_engine("mysql+mysqldb://root:password@@localhost/ghc_gama")

##Uploading the article data to database
from sqlalchemy import create_engine
engine = create_engine('mysql+mysqlconnector://root:Creative@372@localhost:3306/ghc_fama', echo=False)
cnx = engine.connect()
charar.to_sql(name='fama_reports', con=cnx, if_exists = 'append', index=False)
data = pd.read_sql('SELECT * FROM fama_reports', cnx)
