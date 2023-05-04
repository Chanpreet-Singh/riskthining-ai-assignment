import os
import pickle
import shutil
import traceback

import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

import constants


class Modeltraining:
    def __init__(self):
        assert constants.processed_data_folder
        assert constants.processed_file_name
        assert constants.model_folder
        assert constants.model_file

    def folder_tasks(self):
        try:
            if not os.path.isdir(constants.model_folder):
                os.mkdir(constants.model_folder)
            else:
                shutil.rmtree(constants.model_folder)
                os.mkdir(constants.model_folder)
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))

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
        try:
            print("Inside training..")
            data['Date'] = pd.to_datetime(data['Date'])
            data.set_index('Date', inplace=True)

            # Remove rows with NaN values
            data.dropna(inplace=True)

            # Select features and target
            features = ['vol_moving_avg', 'adj_close_rolling_med']
            target = 'Volume'

            X = data[features]
            y = data[target]

            # Split data into train and test sets
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


            # Create a RandomForestRegressor model
            model = RandomForestRegressor(n_estimators=100, random_state=42, verbose=1, n_jobs=-1)

            print("Started training..")
            # Train the model
            model.fit(X_train, y_train)

            print("Training complete!")
            # Make predictions on test data
            y_pred = model.predict(X_test)

            # Calculate the Mean Absolute Error and Mean Squared Error
            mae = mean_absolute_error(y_test, y_pred)
            mse = mean_squared_error(y_test, y_pred)
            print("Mean absolute error : {0}\nMean squared error : {1}".format(mae, mse))
            self.save_model(model)
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))

    def main(self):
        self.folder_tasks()

        data = self.read_parquet_file(os.path.join(constants.processed_data_folder, constants.processed_file_name))
        assert not data.empty, "Blank file - {0}".format(os.path.join(constants.processed_data_folder, constants.processed_file_name))

        self.perform_training_data(data)

if __name__ == "__main__":
    cls_obj = Modeltraining()
    cls_obj.main()
