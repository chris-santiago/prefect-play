import time

from prefect import task, flow
from prefect.task_runners import SequentialTaskRunner


@task
def task_3_sec():
    time.sleep(5)


@task
def task_2_sec():
    time.sleep(2)


@task
def task_7_sec():
    time.sleep(7)


@task
def task_final():
    print('All tasks complete!')


@flow(name='sequential-run', task_runner=SequentialTaskRunner())
def my_flow():
    start = time.perf_counter()
    res_1 = task_3_sec()
    res_2 = task_7_sec()
    res_3 = task_2_sec(wait_for=[res_2])
    res_4 = task_2_sec(wait_for=[res_2])
    res_5 = task_2_sec(wait_for=[res_1, res_3])
    final = task_final(wait_for=[res_4, res_5])
    duration = time.perf_counter() - start
    print(f'Flow took {duration} seconds.')


if __name__ == '__main__':
    my_flow()
