
# coding: utf-8

# author:  WU Nan
# Date: Nov 30, 2016

from __future__ import print_function
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datetime import datetime
import sys
from operator import add
from pyspark import SparkConf, SparkContext

import numpy as np
import csv
import math



def getdateToEs(filename):    
    filename = "file:/Users/nancywu/sparkhadoop/datatest/"+filename+".csv"
    File = sc.textFile(filename)
    File.map(lambda line: line.split(","))
    File.filter(lambda line: len(line) > 0)
    File.map(lambda line: (line[0], line[1]))
    #print(File.first())
    data = File.collect()
    df = [d.split(",") for d in data]
    j = 1
    actions = []
    count = int(len(df))
    while (j < count):
        action = {
                   "_index": "stock", # 这里不可以是大写，都是小写
                   "_type": "AA",
                   "_id": j,
                   "_source": {
                               "date":df[j][0],
                               "open":float(df[j][1]),
                               "high":float(df[j][2]),
                               "low":float(df[j][3]),
                               "close":float(df[j][4]),
                               "volume":int(df[j][5]),
                               "adjClose":float(df[j][6]),
                               #"timestamp": datetime.now()
                                }
                   }
        print(action)
        actions.append(action)
        j += 1
        if (len(actions) == 180):
            helpers.bulk(es, actions)
            del actions[0:len(actions)]

    

if __name__ =="__main__":
    sc = SparkContext(appName="Monte Carlo")
    es=Elasticsearch()
    getdateToEs("AA")



