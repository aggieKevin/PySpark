# -*- coding: utf-8 -*-
"""
Created on Sat Apr 27 10:23:10 2019

@author: hejia
"""

from pyspark import SparkConf, SparkContext
import os

root=r'C:\Users\hejia\Documents\python\python-spark\spark_udemy_taming_big_data'
file_name='book.txt'

# setting
conf=SparkConf().setMaster('local').setAppName('word_count_pra')
sc=SparkContext(conf=conf)

lines=sc.textFile(os.path.join(root,file_name))
words=lines.flatMap(lambda x: x.split())
result=words.countByValue()
result=sorted(result.items(),key=lambda x: x[1])
for word,count in result:
    cleanWord = word.encode('ascii', 'ignore')
    print(cleanWord.decode(),count)
sc.stop()
