from pyspark import SparkConf, SparkContext
import os
conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)

def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))
root=r'C:\Users\hejia\Documents\python\python-spark\spark_udemy_taming_big_data'

names = sc.textFile(os.path.join(root,"marvel-names.txt"))
namesRdd = names.map(parseNames)

lines = sc.textFile(os.path.join(root,"marvel-graph.txt"))

pairings = lines.map(countCoOccurences)
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
flipped = totalFriendsByCharacter.map(lambda xy : (xy[1], xy[0]))

mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0].decode()

print(str(mostPopularName) + " is the most popular superhero, with " + \
    str(mostPopular[0]) + " co-appearances.")
sc.stop()