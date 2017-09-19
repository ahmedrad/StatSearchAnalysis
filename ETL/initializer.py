import os

import findspark
findspark.init()

import pyspark
sc = pyspark.SparkContext(appName='Assignment')

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

def get_out_dir():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    parent_dir = os.path.dirname(current_dir)
    return os.path.join(parent_dir, 'out')
