# -*- coding: utf-8 -*-
"""
Created on Sat Apr 27 15:04:07 2019

@author: hejia
"""

from pyspark import SparkConf, SparkContext
import os

def getNames():
    file=r'ml_100k\u.item'
    movieNames=dict()
    with open(file) as f:
        for line in f:
            values=line.split('|')
            no=int(values[0])
            name=values[1]
            movieNames[no]=name
    return movieNames

conf=SparkConf().setMaster('local').setAppName('movie')
sc=SparkContext(conf=conf)
nameDict=sc.broadcast(getNames())
root=r'C:\Users\hejia\Documents\python\python-spark\spark_udemy_taming_big_data'
lines=sc.textFile(os.path.join(root,r'ml_100k\u.data'))
movies=lines.map(lambda x: (int(x.split()[0]),1))
movies2=movies.reduceByKey(lambda x, y: x+y)
movies3=movies2.map(lambda x: (x[1],x[0]))
movies4=movies3.sortByKey()
movies5=movies4.map(lambda x: (nameDict.value[x[1]],x[0]))
result=movies5.collect()
sc.stop()
            