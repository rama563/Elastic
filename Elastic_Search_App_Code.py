# -*- coding: utf-8 -*-
"""
Created on Mon Jun 11 13:51:50 2018
@author: RamaKrishna
"""

##Importing required libraries
import requests
import os
import pandas as pd
from elasticsearch import Elasticsearch
from flask import Flask, redirect, url_for, request,render_template

## Setting up working directory
os.chdir("K:/HE Project - FAMA/Elastic Search Code/PythonApp")

##Parameter to fix the length issue in Pandas dataframe
pd.set_option('display.max_colwidth', -1)

##Creating an empty Flask application
app = Flask(__name__,static_url_path='')

##Making application to read data from static location 
@app.route('/')
def root():
    return app.send_static_file('index.html')

##This is dummy. will be useful when we handle post requests
@app.route('/success/<name>')
def success(name):
   return 'welcome %s' % name  #Not Required. We are only using get here.

##Getting the search text, calling search function and rendering search results on to the html page 
@app.route('/index',methods = ['POST', 'GET'])
def index():
   if request.method == 'POST':
      string = request.form['Search_Text']
      print(string)
      p=search(string)
      return redirect(url_for('success',name =string)) #Not Required. We are only using get here.
   else:
      string = request.args.get('Search_Text')
      p=search(string)
      return render_template('index.html', tables = p.to_html(justify='center',escape=False,index=False),titles = ["Search Results"]) 

##Search function
def search(search_string):
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    res = requests.get('http://localhost:9200')
    print(res.content)
    documents_op = es.search(index='helloworld',body={
    "query" : {
        "match_phrase" : {
            "Content" : search_string
        }
    },
    "highlight": {
           # "boundary_chars":".",
           "boundary_max_scan":1500,
           "fragment_size":1000,
        "fields" : {        
            "Content" : {}                     ###"type" : "unified"
        }
    }
    })['hits']['hits']


##Checking and formatting output
    Output_len=len(documents_op)
    if(Output_len>0):
        for i in range(0,Output_len):
            temp=documents_op[i]
          #  temp_len=len(temp["highlight"]["Content"])
            temp_ticker=temp["_source"]["Ticker"]
            temp_YQ=temp["_source"]["Year_Quarter"]
            temp_RT=temp["_source"]["Report_Type"]
            if(temp_RT=='PR'):
                temp_RT="Press Release"
            else:
                temp_RT="Earnings Transcript"
            output_pd=pd.DataFrame(temp["highlight"]["Content"],columns=['Relevant_Paragraph'])
            output_pd["Company Ticker"]=temp_ticker
            output_pd["Year_Quarter"]=temp_YQ
            output_pd["Report_Name"]=temp_RT
            
            if(i==0):    
                final_output=output_pd
            else:
                final_output=final_output.append(output_pd,ignore_index=True)
        fp=final_output[["Company Ticker","Year_Quarter","Report_Name","Relevant_Paragraph"]]
        for j in range(0,len(fp)):
            fp["Relevant_Paragraph"][j] = " || ".join(fp["Relevant_Paragraph"][j].split("\n\n"))
            fp["Relevant_Paragraph"][j] = " ".join(fp["Relevant_Paragraph"][j].split("\n"))   
    else:
        print("No matches found in the documents")
        fp= pd.DataFrame(["No Matches Found"],columns=['Oops'])
    
    return fp

##Running the app
if __name__ == '__main__':
   app.run()
  