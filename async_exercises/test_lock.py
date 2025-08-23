import asyncio
import time

# Shared resource
counter = 0

async def worker_without_lock(name):
    global counter
    print(f"{name} starting work...")
    
    # Simulate some work
    await asyncio.sleep(0.1)
    
    # Critical section - reading and writing shared variable
    current = counter
    await asyncio.sleep(0.1)  # Simulate some processing
    counter = current + 1
    
    print(f"{name} finished. Counter: {counter}")

async def worker_with_lock(name, lock):
    global counter
    print(f"{name} starting work...")
    
    async with lock:  # Only one task can enter this block at a time
        # Critical section
        current = counter
        await asyncio.sleep(0.1)  # Simulate some processing
        counter = current + 1
        print(f"{name} finished. Counter: {counter}")

async def main():
    global counter
    
    print("=== WITHOUT LOCK (RACE CONDITION) ===")
    counter = 0
    tasks = [worker_without_lock(f"Worker {i}") for i in range(5)]
    await asyncio.gather(*tasks)
    print(f"Final counter: {counter} (should be 5, but might be less!)\n")
    
    print("=== WITH LOCK (NO RACE CONDITION) ===")
    counter = 0
    lock = asyncio.Lock()
    tasks = [worker_with_lock(f"Worker {i}", lock) for i in range(5)]
    await asyncio.gather(*tasks)
    print(f"Final counter: {counter} (should be 5)")

asyncio.run(main())