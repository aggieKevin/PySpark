'''
from pyspark import SparkConf, SparkContext
import collections
import os

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

#lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
root=r'C:\Users\hejia\Documents\python\python-spark\spark_udemy_taming_big_data\ml_100k'
file=os.path.join(root,"u.data")
lines = sc.textFile(file)
# a row is a value
ratings = lines.map(lambda x: x.split()[2]) # creating a new RDD
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
sc.stop()
'''

#spark-submit ratings-counter.py

from pyspark import SparkConf, SparkContext
import collections
import os

conf=SparkConf().setMaster('local').setAppName('RatingHistogram')
sc=SparkContext(conf=conf)

root=r'C:\Users\hejia\Documents\python\python-spark\spark_udemy_taming_big_data\ml_100k'
file=os.path.join(root,"u.data")
# create a rdd by reading the data from file
lines=sc.textFile(file)

ratings=lines.map(lambda x: x.split()[2])
result=ratings.countByValue()

sorted_result=sorted(result.items())
for value,count in sorted_result:
    print('{} has {}'.format(value,count))
sc.stop()

