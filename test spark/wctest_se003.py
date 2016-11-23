from __future__ import print_function
#from elasticsearch import Elasticsearch
import sys
from operator import add
from pyspark import SparkContext

import numpy as np
import csv
import math

if __name__=="__main__":
    sc = SparkContext(appName="word count")
    text_file = sc.textFile("hdfs:///nwtest/wctest/book.txt")
    output=text_file.count()
    print ("----------How many lines(total no. of rdd)--------")
    print (output)
  
    print ("----------How many times shown in each word--------")
    count1 = text_file.flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
            .reduceByKey(lambda a, b: a + b)
    output=count1.collect()
    #print (output)
    count_total=0
    for (words, counts) in output:
        print (words, counts)
        count_total+=counts
    
    print ("------------How many different words in doc------------")
    x=sc.parallelize(output)
    counts_eachword=x.count()
    print (counts_eachword)

print ("------------Total No. of Words------------")
    print (count_total)
    count1.saveAsTextFile("hdfs:///nwtest/wctest/result.txt")
    sc.stop()





