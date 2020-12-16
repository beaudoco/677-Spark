# Word Count program

from __future__ import print_function
import sys
import time
import glob
from operator import add
import re
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def mapper(word):
    return (word[87:92])


if __name__ == "__main__":
    start_time = time.time()
    sc = SparkContext(appName="PySparkClimate")
#     f = open("1980.txt", "w")
    # for fileName in glob.glob("/home/DATA/NOAA_weather/1980/*.gz"):
    lines = sc.textFile("/home/DATA/NOAA_weather/1980/325960-99999-1980.gz", 1)
    counts = lines.flatMap(lambda x: x.splitlines()) \
        .map(mapper) \
        .filter(lambda x: x < "9999") \
        .mean()
    output = counts.collect()
    end_time = time.time()
    print(output)
    print("TIME OF PROGRAM: ", end_time - start_time)
#     f.write(str(stats[0]['mean']) + ",")
#     f.close()
    sc.stop()
