import gc
import subprocess
from prefect import flow, task

python_prefix = "python3.8"
spark_prefix = "spark-submit"

@task
def problem_1a():
    exec_status = False
    proc = subprocess.call([python_prefix, "problem-1-a.py"])
    if proc == 0:
        exec_status = True
    gc.collect(1)
    return exec_status

@task
def problem_1b():
    print("Starting problem-1-b")
    exec_status = False
    proc = subprocess.call([spark_prefix, "problem-1-b.py"])
    if proc == 0:
        exec_status = True
    gc.collect(1)
    return exec_status

@task
def problem_2():
    print("Starting problem-2")
    exec_status = False
    proc = subprocess.call([spark_prefix, "problem-2.py"])
    if proc == 0:
        exec_status = True
    gc.collect(1)
    return exec_status

@task
def problem_3():
    print("Starting problem-3")
    exec_status = False
    proc = subprocess.call([python_prefix, "problem-3.py"])
    if proc == 0:
        exec_status = True
    return exec_status

@flow(name="Riskthinking.ai Assignment Data Pipeline Flow")
def riskthinking_assignment_master():
    a = problem_1a.submit()
    assert a.result(), "Execution of problem_1a failed!"
    print("\n"*25)

    b = problem_1b.submit(wait_for=[a])
    assert b.result(), "Execution of problem_1b failed!"
    print("\n" * 25)

    c = problem_2.submit(wait_for=[b])
    assert c.result(), "Execution of problem_2 failed!"
    print("\n" * 25)

    d = problem_3.submit(wait_for=[c])
    assert d.result(), "Execution of problem_3 failed!"

riskthinking_assignment_master()
