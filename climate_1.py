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
    return (word[0:4], word[4:10], word[10:15], word[15:23], word[23:27], word[27:28], word[28:34], word[34:41], word[41:46], word[46:51], word[51:56], word[56:60], word[60:63], word[63:64], word[64:65], word[65:69], word[69:70], word[70:75], word[75:76], word[76:77], word[77:78], word[78:84], word[84:85], word[85:86], word[86:87], word[87:92], word[92:93], word[93:98], word[98:99], word[99:104], word[104:105])

def reducer(x, y):
    return x+y

if __name__ == "__main__":
    start_time = time.time()
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PySparkClimate")
    f = open("1980.txt", "w")
    for fileName in glob.glob("/home/DATA/NOAA_weather/1980/*.gz"):
        lines = sc.textFile(fileName, 1)
        counts = lines.flatMap(lambda x: x.splitlines()) \
            .map(mapper)  # \
        # .reduceByKey(reducer)
        output = counts.collect()
        # end_time = time.time()
        # print("TIME OF PROGRAM: ", end_time - start_time)
        spark = SparkSession.builder \
            .master("local") \
            .appName("Word Count") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
        df = spark.createDataFrame(output, ['VARS', 'USAF_CAT_ID', 'NCDC_WBAN_ID', 'OBSV_DATE', 'OBSV_TIME', 'OBSV_FLAG', 'OBSV_LAT', 'OBSV_LONG', 'RPT_TYPE', 'OBSV_ELEV', 'STAT_LET_ID', 'OBSV_QC_PROC', 'OBSV_ANG', 'OBSV_QUAL_CODE', 'OBSV_TYPE', 'OBSV_SPEED',
                                            'OBSV_SPEED_QUAL_CODE', 'OBSV_CEIL_H', 'OBSV_CEIL_QUAL_CODE', 'OBSV_CEIL_DET_CODE', 'CAVOK_CODE', 'VIS_D', 'VIS_D_QUAL', 'VIS_VAR', 'VIS_QUAL_VAR_CODE', 'AIR_TMP', 'AIR_TMP_Q', 'AIR_TMP_DEW', 'AIR_TMP_DEW_QUAL', 'SEA_PRESS', 'SEA_PRESS_QC'])
        # df.describe(['AIR_TMP']).show()
        df = df.where(F.col('AIR_TMP') != 9999)
        stats = df.select(F.mean(F.col('AIR_TMP')).alias('mean')).collect()
        # print(stats[0]['mean'])
        f.write(str(stats[0]['mean']) + ",")
    end_time = time.time()
    print("TIME OF PROGRAM: ", end_time - start_time)
    f.close()
    sc.stop()
