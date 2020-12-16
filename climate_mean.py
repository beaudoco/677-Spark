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
    return (float(word))


if __name__ == "__main__":
    start_time = time.time()
    year_list = ['1980', '1981', '1982', '1983', '1984', '1985', '1986', '1987', '1988', '1989', '1990', '1991', '1992', '1993', '1994', '1995',
                 '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012']
    # year_list = ['1980']
#   SPARK CONTEXT
    sc = SparkContext(appName="PySparkClimate1980")
    f = open("mean.txt", "w")
#   LOOP THRU EACH YEAR
    for year in year_list:
#         f = open(year + ".txt", "w")
        for fileName in glob.glob("" + year + ".txt"):
            lines = sc.textFile(fileName, 1)
            counts = lines.flatMap(lambda x: x.split(',')) \
                .map(mapper) \
                .mean()
            output = counts
#           ADD YEARS MEAN TO FILE
            f.write(str(output) + ",")
        f.close()
    end_time = time.time()
    print("TIME OF PROGRAM: ", end_time - start_time)
    f.close()
    sc.stop()
