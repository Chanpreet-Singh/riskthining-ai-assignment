"""This script will perform intermediate folder operations, and perform the mapping of symbol and name for each file. Each file is considered as an intermediate file, which in turn will act as input for the part-b of problem 1."""

import os
import time
import shutil
import traceback
import multiprocessing

import pandas as pd

import constants


class Problem1a:
    def __init__(self):
        assert constants.input_folder
        assert constants.metadata_file
        assert constants.etfs_folder
        assert constants.stocks_folder
        assert constants.semi_processed_data_folder

    def create_folder(self, folder_path, check):
        try:
            if check:
                if not os.path.isdir(folder_path):
                    os.mkdir(folder_path)
                else:
                    shutil.rmtree(folder_path)
                    os.mkdir(folder_path)
            else:
                os.mkdir(folder_path)
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))

    def folder_tasks(self):
        try:
            # Creation of semi-processed/ folder
            self.create_folder(constants.semi_processed_data_folder, check=True)

            # Creation of semi-processed/etfs/ folder
            self.create_folder(os.path.join(constants.semi_processed_data_folder, constants.etfs_folder), check=True)

            # Creation of semi-processed/stocks/ folder
            self.create_folder(os.path.join(constants.semi_processed_data_folder, constants.stocks_folder), check=True)
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))

def set_folder_permissions():
    try:
        folder_paths = [constants.semi_processed_data_folder]
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

def read_csv_file(file_path):
    data = pd.DataFrame()
    try:
        data = pd.read_csv(file_path)
    except FileNotFoundError:
        pass
    except Exception as e:
        print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
    return data

global metadata
metadata = read_csv_file(os.path.join(constants.input_folder, constants.metadata_file))
assert not metadata.empty, "Without metadata, this script cannot proceed!"
def dump_csv_file(intermediate_data, folder_name, each_symbol):
    try:
        intermediate_data.to_csv(os.path.join(constants.semi_processed_data_folder, folder_name, each_symbol + ".csv"), header=True, index=False)
    except Exception as e:
        print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))

def insert_symbol_and_name(folder_name, symbol):
    file_data = read_csv_file(os.path.join(constants.input_folder, folder_name, symbol + ".csv"))
    if not file_data.empty:
        total_records = len(file_data)
        file_data.insert(0, "Symbol", [symbol] * total_records)
        file_data.insert(1, "Security Name", [metadata[metadata["Symbol"] == symbol]["Security Name"].iloc[0]] * total_records)
    return file_data
def process_file_etf(file_name):
    try:
        intermediate_data = insert_symbol_and_name(constants.etfs_folder, file_name)
        if not intermediate_data.empty:
            dump_csv_file(intermediate_data, constants.etfs_folder, file_name)
        else:
            return "No data"
    except Exception as e:
        print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))

def process_file_stocks(file_name):
    try:
        intermediate_data = insert_symbol_and_name(constants.stocks_folder, file_name)
        if not intermediate_data.empty:
            dump_csv_file(intermediate_data, constants.stocks_folder, file_name)
    except Exception as e:
        print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
def process_folder(folder_name):
    global metadata
    try:
        processor_pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
        if folder_name == "etfs":
            etf_metadata = metadata[metadata['ETF'] == 'Y']
            file_list = etf_metadata['Symbol'].tolist()
            print("Found {0} companies/files for ETFs in metadata".format(len(file_list)))
            processor_pool.map(process_file_etf, file_list)
        else:
            stocks_metadata = metadata[metadata['ETF'] == 'N']
            file_list = stocks_metadata['Symbol'].tolist()
            print("Found {0} companies/files for Stocks in metadata".format(len(file_list)))
            processor_pool.map(process_file_stocks, file_list)
        processor_pool.close()
        processor_pool.join()
    except Exception as e:
        print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))

def print_unprocessed():
    global metadata
    etf_metadata = metadata[metadata['ETF'] == 'Y']
    file_list = etf_metadata['Symbol'].tolist()
    etf_files = [each.strip(".csv") for each in os.listdir(os.path.join(constants.input_folder, constants.etfs_folder))]
    file_not_found = list(set(file_list).difference(set(etf_files)))
    file_not_found = [os.path.join(constants.input_folder, constants.etfs_folder, each.strip(".csv")) for each in file_not_found]

    stocks_metadata = metadata[metadata['ETF'] == 'N']
    file_list = stocks_metadata['Symbol'].tolist()
    stocks_files = [each.strip(".csv") for each in os.listdir(os.path.join(constants.input_folder, constants.stocks_folder))]
    x = [os.path.join(constants.input_folder, constants.stocks_folder, each.strip(".csv")) for each in list(set(file_list).difference(set(stocks_files)))]

    file_not_found.extend(x)
    print("\n\n\n\n\n{0} files as per metadata, were not found".format(len(x)))
    for each in file_not_found:
        print(each)

def main():
    st = time.time()
    print("Doing etfs..")
    process_folder(constants.etfs_folder)
    print("Doing stocks..")
    process_folder(constants.stocks_folder)
    print("Total time taken: {0}s".format(int(time.time() - st)))
    print_unprocessed()
    set_folder_permissions()

if __name__ == "__main__":
    cls_obj = Problem1a().folder_tasks()
    main()
