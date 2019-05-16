# -*- coding: utf-8 -*-
"""
Created on Sat Apr 27 08:55:54 2019

@author: hejia
"""
from pyspark import SparkConf, SparkContext
import os
# data: get location,tp, temp
def getValues(line):
    values=line.split(',')
    name=values[0]
    tp=values[2]
    tem=int(values[3])
    return name, tp,tem
    

conf=SparkConf().setMaster('local').setAppName('find_minimum')
sc=SparkContext(conf=conf)  

root=r'C:\Users\hejia\Documents\python\python-spark\spark_udemy_taming_big_data'

lines =sc.textFile(os.path.join(root,'1800.csv'))
data=lines.map(getValues)

# filter data: get min
min_data=data.filter(lambda x: 'TMIN' in x)

# exclued tp column
sta_tem=min_data.map(lambda x: (x[0],x[2]))
final_min=sta_tem.reduceByKey(lambda x,y :min(x,y))
final_result=final_min.collect()

for station, tem in final_result:
    print(station+ '\t{:.2f}'.format(tem))
sc.stop()
