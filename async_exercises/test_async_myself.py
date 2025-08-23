"""
In your example:
1. Process
One Python process running your script
Contains the Python interpreter, your code, and the event loop

2. Task
asyncio.create_task() or coroutines in gather() create Tasks
In your code:
func3() becomes a Task when passed to gather()
asyncio.sleep(1) becomes a Task when passed to gather()
asyncio.to_thread(func2) creates a Task that manages the thread execution

3. Thread
Main thread: runs the event loop and all coroutines
Worker thread: created by asyncio.to_thread(func2) to run the blocking func2() function
Python's GIL (Global Interpreter Lock) means only one thread runs Python code at a time, but I/O operations can happen concurrently

4. Scheduler (Event Loop)
The asyncio event loop is the scheduler
It manages all Tasks and decides which one to run next
When a Task hits await, the scheduler switches to another ready Task
In your code, the scheduler coordinates:
The func3() coroutine (sleeps 1s)
The asyncio.sleep(1) coroutine (sleeps 1s)
The asyncio.to_thread(func2) Task (runs func2 in a thread)

Process (Python interpreter)
├── Main Thread (Event Loop/Scheduler)
│   ├── Task 1: func3() coroutine
│   ├── Task 2: asyncio.sleep(1) coroutine
│   └── Task 3: to_thread(func2) coordinator
└── Worker Thread (created by to_thread)
    └── func2() execution
"""
import asyncio
import time
from loguru import logger
import numpy as np

def compute_intensive(shape=1000):
    x = np.random.randn(shape,1000)
    return np.matmul(x, x.T)

def func2():
    logger.info('Hello from func2')
    time.sleep(2)

async def func3(): # async creates a coroutine object. You will have to await to get the final result
    logger.info('Hello from func3') 
    await asyncio.sleep(1)
    result = await asyncio.to_thread(compute_intensive, 10)
    logger.info('hello again from func3, with result')
    logger.info(result.shape)

async def main():
    logger.info('hello world')
    # await asyncio.sleep(1)
    await asyncio.gather(
        asyncio.sleep(1),
        func3(),
        # to_thread(fn, …): For blocking synchronous functions only. Don’t pass async functions here.
        asyncio.to_thread(func2), # runs concurrently in a thread
        asyncio.to_thread(func2) # runs concurrently in a thread
    )
    logger.info('ended')
    # await func3() # this is still synchronous, because there is no scheduling by event loop when you .gather
    # await func3() # this is still synchronous, because there is no scheduling by event loop when you .gather
    return 'Hello'


# The Main Thread
#The main thread is the single thread that runs the entire asyncio event loop and all your coroutines.
asyncio.run(main()) # asyncio.run(main())  # This runs on the main thread