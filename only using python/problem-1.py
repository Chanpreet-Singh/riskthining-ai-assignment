import os
import gc
import json
import string
import shutil
import traceback

import pandas as pd
from Levenshtein import distance

import constants


class Preprocess:
    def __init__(self):
        assert constants.input_folder
        assert constants.metadata_file
        assert constants.etfs_folder
        assert constants.stocks_folder
        assert constants.semi_processed_data_folder
        self.file_not_found = []

    def folder_tasks(self):
        try:
            if not os.path.isdir(constants.semi_processed_data_folder):
                os.mkdir(constants.semi_processed_data_folder)
            else:
                shutil.rmtree(constants.semi_processed_data_folder)
                os.mkdir(constants.semi_processed_data_folder)
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))

    def read_csv_file(self, file_path):
        data = pd.DataFrame()
        try:
            data = pd.read_csv(file_path)
        except FileNotFoundError:
            self.file_not_found.append(file_path)
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
        return data

    def insert_symbol_and_name(self, folder_name, file_list, metadata, batch_name):
        final_data = pd.DataFrame()
        total_files = len(file_list)
        for ind, each_symbol in enumerate(file_list):
            try:
                print("{3} - {0}/{1} - {2}".format(ind + 1, total_files, each_symbol, batch_name))
                each_data = self.read_csv_file(os.path.join(constants.input_folder, folder_name, each_symbol + ".csv"))
                each_data.insert(0, "Symbol", [each_symbol] * len(each_data))
                each_data.insert(1, "Security Name", [metadata[metadata["Symbol"] == each_symbol]["Security Name"].iloc[0]] * len(each_data))
                final_data = pd.concat([final_data, each_data])
            except Exception as e:
                print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
        return final_data

    def process_folder(self, folder_name, metadata):
        file_list = []
        if folder_name == "etfs":
            file_list = metadata[metadata['ETF'] == 'Y']['Symbol'].tolist()
            print("Found {0} companies/files for ETFs in metadata".format(len(file_list)))
        else:
            file_list = metadata[metadata['ETF'] == 'N']['Symbol'].tolist()
            print("Found {0} companies/files for Stocks in metadata".format(len(file_list)))

        start = 0
        batch = 100
        final_data = pd.DataFrame()
        while file_list[start: start + batch]:
            interimediate_data = self.insert_symbol_and_name(folder_name, file_list[start: start + batch], metadata, "{0}-{1}".format(start, start + batch))
            final_data = pd.concat([final_data, interimediate_data])
            start += batch
            gc.collect(1)
        return final_data

    def find_total_punctuation(self, file_name):
        count = 0
        try:
            for each_letter in file_name:
                if each_letter in string.punctuation:
                    count += 1
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
        return count

    def find_short_word(self, word):
        short_word = ""
        try:
            for each_letter in word:
                if each_letter in string.punctuation:
                    break
                else:
                    short_word += each_letter
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
        return short_word

    def find_using_levenshtein(self):
        try:
            error_file_dict = dict()
            word_lev_dict = dict()
            relevant_files_dict = dict()
            for each in set(self.file_not_found):
                folder_name = each.split("/")[1]
                file_name = each.split("/")[-1]
                word_lev_dict[file_name] = {
                                                "lev_dist" : self.find_total_punctuation(file_name),
                                                "short_word": self.find_short_word(file_name)
                                            }
                if error_file_dict.get(folder_name, False):
                    if file_name not in error_file_dict[folder_name]:
                        error_file_dict[folder_name].append(file_name)
                else:
                    error_file_dict[folder_name] = [file_name]

            for each_folder in error_file_dict:
                all_files = os.listdir(os.path.join(constants.input_folder, each_folder))
                for each_file in error_file_dict[each_folder]:
                    relevant_files = [each.strip() for each in all_files if each.startswith(word_lev_dict[each_file]["short_word"]) and distance(each_file, each) <= word_lev_dict[each_file]["lev_dist"]]
                    relevant_files_dict[os.path.join(constants.input_folder, each_folder, each_file)] = relevant_files
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
        return relevant_files_dict

    def unprocessed_suggestions(self, metadata):
        print("\n\nThe following files(with their exact names) weren't found! Here are some of the other relevant unprocessed files:-")
        new_unprocessed_files = self.find_using_levenshtein()
        all_symbols = [each.strip() for each in metadata["Symbol"]]
        for each in new_unprocessed_files:
            unprocessed = []
            for each_relevant_file in new_unprocessed_files[each]:
                if each_relevant_file.strip(".csv") not in all_symbols:
                    unprocessed.append(os.path.join(constants.input_folder, each.split("/")[1], each_relevant_file))
            print("{0} -> {1}".format(each, ", ".join(unprocessed)))

    def main(self):
        self.folder_tasks()

        metadata = self.read_csv_file(os.path.join(constants.input_folder, constants.metadata_file))
        assert not metadata.empty, "Without metadata, this script cannot proceed!"

        etf_data = self.process_folder(constants.etfs_folder, metadata)
        assert not etf_data.empty, "Etf's consolidated data, cannot be empty"

        stocks_data = self.process_folder(constants.stocks_folder, metadata)
        assert not stocks_data.empty, "Stock's consolidated data, cannot be empty"

        final_data = pd.concat([etf_data, stocks_data])
        del (etf_data)
        del (stocks_data)

        # There are some files as per metadata["Symbols"] but not found in the folder. This function will give you relevant files paths to those not found.
        self.unprocessed_suggestions(metadata)

        final_data.to_parquet(os.path.join(constants.semi_processed_data_folder, constants.semi_processed_file_name))


if __name__ == "__main__":
    cls_obj = Preprocess()
    cls_obj.main()
