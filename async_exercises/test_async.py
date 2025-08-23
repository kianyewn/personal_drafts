import asyncio
import time

# Synchronous version (blocking)
def sync_task(name, duration):
    print(f"{name} starting at {time.time():.2f}")
    time.sleep(duration)  # This blocks everything
    print(f"{name} finished at {time.time():.2f}")
    return f"{name} result"

# Asynchronous version (non-blocking)
async def async_task(name, duration):
    print(f"{name} starting at {time.time():.2f}")
    await asyncio.sleep(duration)  # This yields control to event loop
    print(f"{name} finished at {time.time():.2f}")
    return f"{name} result"

# Run synchronous tasks
print("=== SYNCHRONOUS EXECUTION ===")
start = time.time()
result1 = sync_task("Task A", 2)
result2 = sync_task("Task B", 2)
end = time.time()
print(f"Synchronous total time: {end - start:.2f} seconds\n")

# Run asynchronous tasks
print("=== ASYNCHRONOUS EXECUTION ===")
async def main():
    start = time.time()
    # These run concurrently!
    result1, result2 = await asyncio.gather(
        async_task("Task A", 2),
        async_task("Task B", 2)
    )
    end = time.time()
    print(f"Asynchronous total time: {end - start:.2f} seconds")
    print(f"Results: {result1}, {result2}")

asyncio.run(main())