import os
import time
import shutil
import traceback

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

def set_folder_permissions():
    try:
        folder_paths = [constants.model_folder]
        permissions = 0o777
        for each_folder_path in folder_paths:
            for root, dirs, files in os.walk(each_folder_path):
                for filename in files:
                    file_path = os.path.join(root, filename)
                    try:
                        os.chmod(file_path, permissions)
                    except Exception as e:
                        print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    try:
                        os.chmod(dir_path, permissions)
                    except Exception as e:
                        print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
    except Exception as e:
        print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))

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
set_folder_permissions()
