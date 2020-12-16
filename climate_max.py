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
    return (float(word[87:92]))

def reducer(x,y):
    if (x > y and float(x) < float("9999")):
        return x
    elif (float(y) < float("9999")):
        return y
    else:
        return 0



if __name__ == "__main__":
    start_time = time.time()
    year_list = ['1995', '1996']
    # year_list = ['1980', '1981']
    sc = SparkContext(appName="PySparkClimate1981")
    for year in year_list:
        f = open(year + "-max.txt", "w")
        for fileName in glob.glob("/home/DATA/NOAA_weather/" + year + "/*.gz"):
            lines = sc.textFile(fileName, 1)
            counts = lines.flatMap(lambda x: x.splitlines()) \
                .map(mapper) \
                .reduce(reducer)
                # .filter(lambda x: float(x) < float("9999")) \
                # .reduce(reducer)
                
            output = counts
            # output = counts.collect()
            # print(output)
            f.write(str(output) + ",")
        f.close()
    end_time = time.time()
    print("TIME OF PROGRAM: ", end_time - start_time)
    sc.stop()
