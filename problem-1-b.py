import os
import time
import shutil

import constants
from pyspark.sql import SparkSession


def folder_validations_and_operations():
    assert constants.semi_processed_data_folder, "input folder to this script doesn't exist!"
    assert constants.etfs_folder
    assert constants.stocks_folder
    assert os.path.isdir(os.path.join(constants.semi_processed_data_folder, constants.etfs_folder)), "etfs folder in {0} folder doesn't exist".format(constants.semi_processed_data_folder)
    assert os.path.isdir(os.path.join(constants.semi_processed_data_folder, constants.stocks_folder)), "stocks folder in {0} folder doesn't exist".format(constants.semi_processed_data_folder)
    assert len(os.listdir(os.path.join(constants.semi_processed_data_folder, constants.etfs_folder))) > 0, "etfs folder in {0} seems empty".format(constants.semi_processed_data_folder)
    assert len(os.listdir(os.path.join(constants.semi_processed_data_folder, constants.stocks_folder))) > 0, "stocks folder in {0} seems empty".format(constants.semi_processed_data_folder)
    assert constants.processed_file_name, "processed file name is empty!"
    if not os.path.isdir(constants.processed_data_folder):
        os.mkdir(constants.processed_data_folder)
    else:
        shutil.rmtree(constants.processed_data_folder)
        os.mkdir(constants.processed_data_folder)

folder_validations_and_operations()

spark = SparkSession.builder.appName("Aggregate CSV and Convert to Parquet").getOrCreate()

etfs_path = "{0}/{1}/*".format(constants.semi_processed_data_folder, constants.etfs_folder)
stocks_path = "{0}/{1}/*".format(constants.semi_processed_data_folder, constants.stocks_folder)
st = time.time()
etfs_df = spark.read.option("header", "true").option("inferSchema", "true").csv(etfs_path)
stocks_df = spark.read.option("header", "true").option("inferSchema", "true").csv(stocks_path)
final_data = etfs_df.union(stocks_df)
final_data = final_data.repartition(30)
final_data.write.mode("overwrite").parquet(os.path.join(constants.processed_data_folder, constants.processed_file_name))
print("\n"*10)
print("Total time taken: {0}s".format(int(time.time() - st)))
spark.stop()