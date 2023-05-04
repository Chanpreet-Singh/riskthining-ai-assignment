import os
import json
import pickle
import shutil
import traceback
import python3_gearman

import pandas as pd

import constants


class Modelusing:
    def __init__(self):
        assert constants.model_folder
        assert constants.model_file
        assert constants.model_gearman_task_name

        self.model = self.load_model()
        assert self.model

        print("Registering {0} worker in gearman job server".format(constants.model_gearman_task_name))
        self.gm_worker = python3_gearman.GearmanWorker(["localhost:4730"])
        self.gm_worker.register_task(constants.model_gearman_task_name, self.worker_init)

    def worker_init(self, gearman_worker, gearman_job):
        op = {"value": None}
        try:
            model_inputs = json.loads(gearman_job.data)
            print("Request for model, vol_moving_avg: {0}\tadj_close_rolling_med: {1}".format(model_inputs["vol_moving_avg"], model_inputs["adj_close_rolling_med"]))
            model_op = self.model.predict([[model_inputs["vol_moving_avg"], model_inputs["adj_close_rolling_med"]]])
            op["value"] = int(model_op[0])
            op = json.dumps(op)
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
        return op

    def load_model(self):
        model = None
        try:
            with open(os.path.join(constants.model_folder, constants.model_file), "rb") as fp:
                model = pickle.load(fp)
        except Exception as e:
            print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
        return model

if __name__ == "__main__":
    cls_obj = Modelusing()
    print("Worker started..\n\n")
    cls_obj.gm_worker.work()
