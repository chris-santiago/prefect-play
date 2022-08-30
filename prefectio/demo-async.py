import time
import asyncio

from prefect import task, flow


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


@flow(name='async-run')
async def my_flow():
    start = time.perf_counter()
    res_1 = await task_3_sec.submit()
    res_2 = await task_7_sec.submit()
    res_3 = await task_2_sec.submit(wait_for=[res_2])
    res_4 = await task_2_sec.submit(wait_for=[res_2])
    res_5 = await task_2_sec.submit(wait_for=[res_1, res_3])
    final = await task_final.submit(wait_for=[res_4, res_5])
    duration = time.perf_counter() - start
    print(f'Flow took {duration} seconds.')


if __name__ == '__main__':
    asyncio.run(my_flow())
