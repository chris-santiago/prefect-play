import time

from prefect import task, flow
from prefect.tasks import task_input_hash


@task(cache_key_fn=task_input_hash)  # caching prevents re-run of shared dependency
def shared_dep():
    time.sleep(10)


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


@flow()
def sub_flow_1():
    res_1 = task_3_sec()
    res_2 = task_7_sec()
    res_3 = shared_dep(wait_for=[res_1])
    res_4 = task_2_sec(wait_for=[res_2])
    res_5 = task_2_sec(wait_for=[res_1, res_3])
    final = task_final(wait_for=[res_4, res_5])


@flow()
def sub_flow_2():
    res_1 = task_3_sec()
    res_2 = task_7_sec()
    res_3 = shared_dep(wait_for=[res_2])
    res_4 = task_2_sec(wait_for=[res_2])
    res_5 = task_2_sec(wait_for=[res_1, res_3])
    final = task_final(wait_for=[res_4, res_5])


@flow(name='multi-flow-run')
def my_flow():
    start = time.perf_counter()
    sub_1 = sub_flow_1()
    sub_2 = sub_flow_2()
    duration = time.perf_counter() - start
    print(f'Flow took {duration} seconds.')


if __name__ == '__main__':
    my_flow()
