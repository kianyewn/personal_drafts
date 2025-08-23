import asyncio
import time
import random

class SimpleBatchingServer:
    def __init__(self):
        self.queue = []
        self.queue_lock = asyncio.Lock()
        self.needs_processing = asyncio.Event() # Class-level event
        self.processing = False
        
    async def add_request(self, request_id):
        """Simulate adding a request to the queue"""
        async with self.queue_lock:
            if len(self.queue) >= 3:  # MAX_QUEUE_SIZE
                raise Exception("Server too busy!")
            
            # Create a task with an event to signal completion
            task = {
                "id": request_id,
                "done_event": asyncio.Event(), # Per-request event
                "time": time.time(),
                "result": None
            }
            
            self.queue.append(task)
            print(f"Request {request_id} added to queue. Queue size: {len(self.queue)}")
            
            # If we have enough items or this is the first item, trigger processing
            if len(self.queue) >= 2:  # MAX_BATCH_SIZE
                self.needs_processing.set() #  # "Hey processor, we have work!"
            elif len(self.queue) == 1:
                # Schedule processing after 1 second if no more requests come
                asyncio.create_task(self._delayed_processing())
        
        # Wait for this request to be processed
        await task["done_event"].wait()
        return task["result"]
    
    async def _delayed_processing(self):
        """Wait 1 second then trigger processing if queue is not empty"""
        await asyncio.sleep(1)
        if self.queue:
            self.needs_processing.set()
    
    async def process_batch(self):
        """Background task that processes batches"""
        while True:
            await self.needs_processing.wait()
            self.needs_processing.clear()
            
            async with self.queue_lock:
                if not self.queue:
                    continue
                
                # Take up to 2 items from queue
                batch = self.queue[:2]
                self.queue = self.queue[2:]
                print(f"Processing batch: {[t['id'] for t in batch]}")
            
            # Simulate batch processing (expensive operation)
            await asyncio.sleep(2)  # Simulate model inference
            
            # Complete all tasks in batch
            for task in batch:
                task["result"] = f"Processed {task['id']}"
                task["done_event"].set()
                print(f"Completed request {task['id']}")

async def client_request(server, request_id):
    """Simulate a client making a request"""
    try:
        print(f"Client {request_id} sending request...")
        result = await server.add_request(request_id)
        print(f"Client {request_id} received: {result}")
    except Exception as e:
        print(f"Client {request_id} got error: {e}")

async def main():
    server = SimpleBatchingServer()
    
    # Start the background processor
    processor_task = asyncio.create_task(server.process_batch())
    
    # Simulate multiple clients making requests
    print("=== SIMULATING BATCHING SERVER ===")
    
    # First batch: 2 requests arrive quickly
    await asyncio.gather(
        client_request(server, "A"),
        client_request(server, "B")
    )
    
    await asyncio.sleep(1)
    
    # Second batch: 1 request arrives, waits for timeout
    await client_request(server, "C")
    
    await asyncio.sleep(1)
    
    # Third batch: 3 requests arrive quickly
    await asyncio.gather(
        client_request(server, "D"),
        client_request(server, "E"),
        client_request(server, "F")
    )
    
    # Cancel the processor
    processor_task.cancel()

asyncio.run(main())