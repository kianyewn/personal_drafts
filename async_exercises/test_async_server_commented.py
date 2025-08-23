import asyncio  # Core async library for Python
import time     # For timestamps
import random   # (Not used in this example, but commonly needed for ML)

class SimpleBatchingServer:
    """
    A server that batches multiple requests together for efficient processing.
    This is a common pattern in ML model serving to improve throughput.
    """
    def __init__(self):
        # Storage for pending requests - each item is a dict with request details
        self.queue = []
        
        # Protects the queue from race conditions when multiple clients access it simultaneously
        # Only one task can hold this lock at a time
        self.queue_lock = asyncio.Lock()
        
        # Signals the background processor when there's work to do
        # Think of this like a "kitchen bell" that rings when orders are ready
        self.needs_processing = asyncio.Event()
        
        # Tracks if we're currently processing (not used in this simple version)
        self.processing = False
        
    async def add_request(self, request_id):
        """
        Adds a new request to the queue and waits for it to be processed.
        This is what clients call when they want to make a request.
        """
        # Enter critical section - only one client can modify the queue at a time
        # This prevents race conditions like two clients trying to add items simultaneously
        async with self.queue_lock:
            # Check if queue is full - this prevents memory overflow
            if len(self.queue) >= 3:  # MAX_QUEUE_SIZE
                raise Exception("Server too busy!")
            
            # Create a task object that represents this specific request
            # Each request gets its own completion event so we can notify just that client
            task = {
                "id": request_id,                    # Unique identifier for this request
                "done_event": asyncio.Event(),       # Will be signaled when this request is complete
                "time": time.time(),                 # When this request was received
                "result": None                       # Will hold the result when processing is done
            }
            
            # Add this request to the end of the queue (FIFO - First In, First Out)
            self.queue.append(task)
            print(f"Request {request_id} added to queue. Queue size: {len(self.queue)}")
            
            # Decision logic for when to start processing:
            # If we have 2 or more requests, process immediately (efficient batching)
            if len(self.queue) >= 2:  # MAX_BATCH_SIZE
                # Signal the background processor that work is ready
                # This wakes up the processor if it's sleeping
                self.needs_processing.set()
            elif len(self.queue) == 1:
                # If this is the first request, schedule a timeout
                # This ensures we don't wait forever for a second request
                # create_task() starts this coroutine running in the background
                asyncio.create_task(self._delayed_processing())
        
        # Exit the critical section - other clients can now access the queue
        
        # Wait for THIS specific request to be processed
        # This is where the client "blocks" until their request is done
        # The processor will call task["done_event"].set() when done
        await task["done_event"].wait()
        
        # Return the result that the processor put in task["result"]
        return task["result"]
    
    async def _delayed_processing(self):
        """
        Waits 1 second then triggers processing if the queue still has items.
        This handles the case where only one request arrives and we don't want to wait forever.
        """
        # Wait for 1 second - this is non-blocking, other tasks can run during this time
        await asyncio.sleep(1)
        
        # Check if there are still items in the queue (another client might have added more)
        # Note: We don't need a lock here because we're only reading
        if self.queue:
            # Signal that processing should start
            self.needs_processing.set()
    
    async def process_batch(self):
        """
        Background task that continuously processes batches of requests.
        This runs forever until cancelled.
        """
        # Infinite loop - this task runs for the entire lifetime of the server
        while True:
            # Wait for the signal that there's work to do
            # This is where the processor "sleeps" when there's nothing to process
            await self.needs_processing.wait()
            
            # Clear the event so we don't immediately process again
            # We'll wait for the next signal
            self.needs_processing.clear()
            
            # Enter critical section to safely modify the queue
            async with self.queue_lock:
                # Double-check that queue isn't empty (defensive programming)
                if not self.queue:
                    continue  # Go back to waiting
                
                # Take up to 2 items from the front of the queue
                # This is the "batching" - we process multiple requests together
                batch = self.queue[:2]      # Take first 2 items
                self.queue = self.queue[2:] # Remove those 2 items from queue
                print(f"Processing batch: {[t['id'] for t in batch]}")
            
            # Exit critical section - other clients can now add new requests
            
            # Simulate the expensive ML model inference
            # In real ML serving, this would be: model.predict(batch)
            await asyncio.sleep(2)  # Simulate model inference
            
            # Mark each request in the batch as complete
            for task in batch:
                # Store the result (in real ML, this would be the model prediction)
                task["result"] = f"Processed {task['id']}"
                
                # Signal that THIS specific request is done
                # This wakes up the client that was waiting for this request
                task["done_event"].set()
                
                print(f"Completed request {task['id']}")

async def client_request(server, request_id):
    """
    Simulates a client making a request to the server.
    In real applications, this would be an HTTP request or gRPC call.
    """
    try:
        print(f"Client {request_id} sending request...")
        # Call the server and wait for the result
        result = await server.add_request(request_id)
        print(f"Client {request_id} received: {result}")
    except Exception as e:
        # Handle errors gracefully
        print(f"Client {request_id} got error: {e}")

async def main():
    """
    Main function that demonstrates the batching server in action.
    """
    # Create a new server instance
    server = SimpleBatchingServer()
    
    # Start the background processor task
    # This task will run continuously in the background
    processor_task = asyncio.create_task(server.process_batch())
    
    # Simulate multiple clients making requests
    print("=== SIMULATING BATCHING SERVER ===")
    
    # First batch: 2 requests arrive quickly (should be processed together)
    # gather() runs both requests concurrently and waits for both to complete
    await asyncio.gather(
        client_request(server, "A"),  # These two will be batched together
        client_request(server, "B")   # because they arrive at the same time
    )
    
    # Wait a bit to see the timing
    await asyncio.sleep(1)
    
    # Second batch: 1 request arrives, waits for timeout
    # This will wait 1 second before processing because no second request arrives
    await client_request(server, "C")
    
    # Wait a bit more
    await asyncio.sleep(1)
    
    # Third batch: 3 requests arrive quickly
    # The first 2 will be processed together, the 3rd will wait for the next batch
    await asyncio.gather(
        client_request(server, "D"),
        client_request(server, "E"),
        client_request(server, "F")
    )
    
    # Clean shutdown: cancel the background processor task
    # This prevents the infinite loop from running forever
    processor_task.cancel()

# Start the entire program
# run() creates an event loop and runs the main() coroutine
asyncio.run(main())