# -*- coding: utf-8 -*-
"""
Created on Fri May 20 20:02:49 2016

@author: tendaimutunhire
"""
from pyspark import SparkContext

logfile = "book.txt"
sc = SparkContext("local", "SimpleApp")
logData = sc.textFile(logfile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
