import os
import gc
import sys
import math
import pickle
import traceback
import multiprocessing

import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

import constants


class Modeltraining:
    def __init__(self):
        assert constants.model_raw_data
        assert constants.model_folder
        assert constants.model_file

    def read_parquet_file(self, file_path):
        data = pd.DataFrame()
        try:
            data = pd.read_parquet(file_path)
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
        return data

    def save_model(self, model):
        try:
            print("Saving model..")
            with open(os.path.join(constants.model_folder, constants.model_file), "wb") as fp:
                pickle.dump(model, fp)
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))

    def perform_training_data(self, data):
        model = None
        try:
            print("Inside training..")
            data.dropna(inplace=True)
            bytes_to_GB = 1024*1024*1024
            old_size = round(sys.getsizeof(data)/bytes_to_GB, 3)
            # I had to do this because the memory consumption by this file at model.fit() function used to go upto 89% and system used to get crashed!
            for column in data:
                if data[column].dtype == "float64":
                    data[column]=pd.to_numeric(data[column], downcast="float")
            print("Size of data reduced from {0} -> {1}".format(old_size, round(sys.getsizeof(data)/bytes_to_GB, 3)))
            data = data.sample(frac=0.1, replace=False)
            print("New size - {0}".format(round(sys.getsizeof(data)/bytes_to_GB, 3)))
            features = ['vol_moving_avg', 'adj_close_rolling_med']
            target = 'Volume'
            processor_to_use = math.floor(multiprocessing.cpu_count()/4) if int(multiprocessing.cpu_count()/4)>0 else 1
            X_train, X_test, y_train, y_test = train_test_split(data[features], data[target], test_size=0.2, random_state=42)
            print("Total Records\nTrain: {0}\nTest: {1}".format(len(X_train), len(X_test)))
            model = RandomForestRegressor(n_estimators=25, random_state=42, verbose=1, n_jobs=processor_to_use)
            print("Started training..")
            model.fit(X_train, y_train)
            print("Training complete!")
            y_pred = model.predict(X_test)
            mae = mean_absolute_error(y_test, y_pred)
            mse = mean_squared_error(y_test, y_pred)
            print("Mean absolute error : {0}\nMean squared error : {1}".format(mae, mse))
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
        return model

    def main(self):
        gc.collect(1)
        data = self.read_parquet_file(os.path.join(constants.model_folder, constants.model_raw_data))
        assert not data.empty, "Blank file - {0}".format(os.path.join(constants.processed_data_folder, constants.processed_file_name))
        model = self.perform_training_data(data)
        assert model, "There was some problem with the training of data"
        self.save_model(model)

if __name__ == "__main__":
    cls_obj = Modeltraining()
    cls_obj.main()
