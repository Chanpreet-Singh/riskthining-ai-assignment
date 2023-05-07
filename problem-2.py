import os
import time
import shutil

import constants

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import avg, percentile_approx


def folder_validations_and_operations():
    assert constants.processed_data_folder, "input folder to this script doesn't exist!"
    assert constants.processed_file_name, "processed file name is empty!"
    if not os.path.isdir(constants.model_folder):
        os.mkdir(constants.model_folder)
    else:
        shutil.rmtree(constants.model_folder)
        os.mkdir(constants.model_folder)

folder_validations_and_operations()
spark = SparkSession.builder.appName("Feature Engineering - Spark").getOrCreate()
st = time.time()
df = spark.read.parquet(os.path.join(constants.processed_data_folder, constants.processed_file_name))
each_window = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-29, 0)
df = df.withColumn("vol_moving_avg", avg("Volume").over(each_window))
df = df.withColumn("adj_close_rolling_med", percentile_approx("Adj Close", 0.5).over(each_window))
features_needed = ['vol_moving_avg', 'adj_close_rolling_med', 'Volume']
features_not_needed = list(set(list(df.columns)).difference(set(features_needed)))
df = df.drop(*features_not_needed)
df.repartition(50)
df.write.mode("overwrite").parquet(os.path.join(constants.model_folder, constants.model_raw_data))
print("\n"*10)
print("Total time taken: {0}s".format(int(time.time() - st)))
spark.stop()
