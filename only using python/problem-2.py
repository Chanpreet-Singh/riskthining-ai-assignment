import os
import shutil
import traceback

import pandas as pd

import constants


class FeatureEngg:
    def __init__(self):
        assert constants.semi_processed_file_name
        assert constants.processed_data_folder
        assert constants.processed_file_name

    def folder_tasks(self):
        try:
            if not os.path.isdir(constants.processed_data_folder):
                os.mkdir(constants.processed_data_folder)
            else:
                shutil.rmtree(constants.processed_data_folder)
                os.mkdir(constants.processed_data_folder)
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))

    def read_parquet_file(self, file_path):
        data = pd.DataFrame()
        try:
            data = pd.read_parquet(file_path)
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
        return data

    def perform_feat_engg(self, data):
        try:
            print("Performing feature engineering!")
            organizations = data.groupby('Symbol')

            rolling_median = organizations["Adj Close"].rolling(30).median()
            data["adj_close_rolling_med"] = rolling_median.reset_index(drop=True)

            rolling_mean = organizations["Volume"].rolling(30).mean()
            data["vol_moving_avg"] = rolling_mean.reset_index(drop=True)
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
        return data

    def dump_processed_data(self, data):
        try:
            data.to_parquet(os.path.join(constants.processed_data_folder, constants.processed_file_name))
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
    def main(self):
        self.folder_tasks()

        data = self.read_parquet_file(os.path.join(constants.semi_processed_data_folder, constants.semi_processed_file_name))
        assert not data.empty, "Blank file - {0}".format(os.path.join(constants.semi_processed_data_folder, constants.semi_processed_file_name))

        all_columns = data.columns
        assert "Symbol" in all_columns, "Column 'symbol' is missing in the data"
        assert "Adj Close" in all_columns, "Column 'Adj Close' is missing in the data"
        assert "Volume" in all_columns, "Column 'Volume' is missing in the data"

        data = self.perform_feat_engg(data)

        self.dump_processed_data(data)

if __name__ == "__main__":
    cls_obj = FeatureEngg()
    cls_obj.main()
