import os
import time
import shutil
import traceback

import constants
from pyspark.sql import SparkSession


def set_folder_permissions():
    try:
        folder_paths = [constants.processed_data_folder]
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
final_data = final_data.repartition(50)
final_data.write.mode("overwrite").parquet(os.path.join(constants.processed_data_folder, constants.processed_file_name))
print("\n"*10)
print("Total time taken: {0}s".format(int(time.time() - st)))
spark.stop()
set_folder_permissions()