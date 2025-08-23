import asyncio
from loguru import logger

async def fetch_data(delay, id):
    logger.info(f'starting from id: {id}')
    await asyncio.sleep(delay)
    data = {'id': id, 'data':f'data from coroutine{id}'}
    return data

shared_resource = 0
async_lock = asyncio.Lock()

async def modify_shared_resource():
    global shared_resource
    async with async_lock:
        print(f'before modification: {shared_resource}')
        shared_resource += 1
        await asyncio.sleep(1)
        print(f'After modifification: {shared_resource}')

async def main():
    # task1 = await fetch_data(1, 1)
    # task2 = await fetch_data(1,2)

    results = await asyncio.gather(fetch_data(1,1), fetch_data(1,2))
    for result in results:
        print(result)

    # task group
    # tasks = []
    # async with asyncio.TaskGroup() as tg:
    #     for i, sleep_time in enumerate([1,2,3]):
    #         task= tg.create_task(fetch_data(sleep_time, id))
    #         tasks.append(task)

    # # After the task group block, all tasks have completed
    # results = [tasks.result() for task in tasks]
    # for result in results:
    #     print(results)
    
asyncio.run(main())
