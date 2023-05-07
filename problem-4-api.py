import json
import traceback
import subprocess
import python3_gearman

import constants

from fastapi import FastAPI

app = FastAPI()

def check_gearman_worker_status():
    error_str = "{0} doesn't exist in gearman server".format(constants.model_gearman_task_name)
    error = True
    try:
        op = subprocess.run(["gearadmin", "--status"], capture_output=True)
        str_op = op.stdout.decode("utf-8")
        x = [each_line for each_line in str_op.split("\n") if each_line.startswith(constants.model_gearman_task_name)]
        if x:
            x = x[0]
            num_workers = int(x.split("\t")[3])
            if num_workers > 0:
                print("{0} instance of {1} gearman worker are running, good to go!\n\n".format(num_workers, constants.model_gearman_task_name))
                error = False
            else:
                error_str = "{0} is registered in gearman server, but no instances".format(constants.model_gearman_task_name)
    except Exception as e:
        print("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
    assert not error, "{0}, {1}\n\n".format(error_str, "API cannot be started")

@app.get("/")
def check():
    return "The API service is healthy!"

@app.get("/predict")
async def use_model(vol_moving_avg: int, adj_close_rolling_med: int):
    api_output = "We are experiencing some problem, please try after some time"
    gm_client = python3_gearman.GearmanClient(['localhost:4730'])
    data = json.dumps({"vol_moving_avg": vol_moving_avg, "adj_close_rolling_med": adj_close_rolling_med})
    output = gm_client.submit_job(constants.model_gearman_task_name, data)
    output = json.loads(output.result)
    if output["value"]:
        api_output = output["value"]
    return api_output


check_gearman_worker_status()
