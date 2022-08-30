import time
import asyncio

from prefect import task, flow
from prefect.tasks import task_input_hash


@task(cache_key_fn=task_input_hash)  # caching prevents re-run of shared dependency
async def shared_dep():
    await asyncio.sleep(10)


@task
async def task_3_sec():
    await asyncio.sleep(5)


@task
async def task_2_sec():
    await asyncio.sleep(2)


@task
async def task_7_sec():
    await asyncio.sleep(7)


@task
async def task_final():
    print('All tasks complete!')


@flow()
async def sub_flow_1():
    res_1 = await task_3_sec.submit()
    res_2 = await task_7_sec.submit()
    res_3 = await shared_dep.submit(wait_for=[res_1])
    res_4 = await task_2_sec.submit(wait_for=[res_2])
    res_5 = await task_2_sec.submit(wait_for=[res_1, res_3])
    final = await task_final.submit(wait_for=[res_4, res_5])


@flow()
async def sub_flow_2():
    res_1 = await task_3_sec.submit()
    res_2 = await task_7_sec.submit()
    res_3 = await shared_dep.submit(wait_for=[res_2])
    res_4 = await task_2_sec.submit(wait_for=[res_2])
    res_5 = await task_2_sec.submit(wait_for=[res_1, res_3])
    final = await task_final.submit(wait_for=[res_4, res_5])


@flow(name='async-multi-flow-run')
async def my_flow():
    start = time.perf_counter()
    await asyncio.gather(sub_flow_1(), sub_flow_2())
    duration = time.perf_counter() - start
    print(f'Flow took {duration} seconds.')


if __name__ == '__main__':
    asyncio.run(my_flow())
