# -*- coding: utf-8 -*-
"""
Created on Sat Apr 27 00:32:47 2019

@author: hejia
"""

from pyspark import SparkConf, SparkContext
import os
import collections

def processLine(line):
    values=line.split(',')
    age=int(values[2])
    friends=int(values[3])
    return age,friends

# define conf
# def sparkcontext
conf=SparkConf().setMaster('local').setAppName('friends_age')
sc=SparkContext(conf=conf)

root=r'C:\Users\hejia\Documents\python\python-spark\spark_udemy_taming_big_data'
lines=sc.textFile(os.path.join(root,'fakefriends.csv'))

# first: just get age and noFriends
age_friends=lines.map(processLine)

# second: age (noFriends,1)
age_friends_1=age_friends.mapValues(lambda x: (x,1))

# get the sum of friends and sum for records
age_group=age_friends_1.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

# get average, group by age
age_fridents_avg=age_group.mapValues(lambda x: x[0]/x[1])

final=age_fridents_avg.collect()

for result in final:
    print(result)
sc.stop()



# values I have
# no, name, age, numbe of friends



# third: reduce by key
# map: noFriends, NO---noFriends /NO
