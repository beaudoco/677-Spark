# Word Count program

from __future__ import print_function

import sys
import time
import glob
from operator import add

from pyspark import SparkContext

def mapper(word):
        return (word, 1)

def reducer(x,y):
        return x+y

if __name__ == "__main__":
    start_time = time.time()
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PySparkClimate")
    for fileName in glob.glob("/home/DATA/NOAA_weather/1980/*.gz"):
            lines = sc.textFile(fileName, 1)
            counts = lines.flatMap(lambda x: x.split(' ')) \
                    .map(mapper) \
                    .reduceByKey(reducer)
            output = counts.collect()
            
            # for (word, count) in output:
            #     print("%s: %i" % (word, count))
        #     print("%s: %i" % output[0])
    end_time = time.time()
    print("TIME OF PROGRAM: ",end_time - start_time)
    sc.stop()