import asyncio
import time

async def waiter(event, name):
    print(f"{name} is waiting for the event...")
    await event.wait()  # Waits until event is set
    print(f"{name} received the event!")

async def setter(event):
    print("Setter is sleeping for 3 seconds...")
    await asyncio.sleep(3)
    print("Setter is setting the event!")
    event.set()  # This wakes up all waiters

async def main():
    # Create an event
    event = asyncio.Event()
    
    # Create multiple waiters
    waiters = [
        waiter(event, "Waiter 1"),
        waiter(event, "Waiter 2"),
        waiter(event, "Waiter 3")
    ]
    
    # Start all tasks
    await asyncio.gather(
        setter(event),
        *waiters
    )

asyncio.run(main())