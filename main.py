import asyncio
from a_certs_firehose.a_certs_firehose import ACertsFirehose
from b_certs_filtering.b_certs_filtering import BCertsFiltering
from dotenv import load_dotenv

load_dotenv()

# Process A: Certstream data intake, running in a separate thread to avoid blocking
async def process_a(queue_ab, cert_counter):
    firehose = ACertsFirehose(queue_ab, cert_counter)
    await firehose.start_listening()

# Process B: Assign numeric value to each character and process
async def process_b(queue_ab, queue_bc):
    # b_certs_filtering = BCertsFiltering(queue_ab, queue_bc)
    while True:
        # Wait for the next certificate to be available in queue_ab (non-blocking)
        all_domains = await queue_ab.get()  # This will block until an item is available
        
        # Example processing of the certificate
        print(f"Processing certificate: {all_domains}")
        
        # filtered_domains = b_certs_filtering.filter(all_domains)
        
        # Put the processed value into queue_bc
        queue_bc.put_nowait(all_domains)  # Non-blocking
        print(f"Processed value {all_domains} added to queue_bc")

# Process C: Save final value to a file
async def process_c(queue_bc):
    with open("output-asyncio.txt", "a") as f:
        while True:
            processed_value = await queue_bc.get()  # Consume data from queue BC
            f.write(f"Processed value: {processed_value}\n")
            f.flush()
            print(f"Process C saved: {processed_value}")

# Process D: Certs received per second statistic
async def process_d(queue_ab, queue_bc, cert_counter):
    while True:
        await asyncio.sleep(1)
        print(f"Queue AB size: {queue_ab.qsize()}, Queue BC size: {queue_bc.qsize()}")
        print(f"Certs received in the last second: {cert_counter[0]}")
        cert_counter[0] = 0  # Reset counter every second

async def main():
    # Creating two FIFO queues
    queue_ab = asyncio.Queue(maxsize=1000)
    queue_bc = asyncio.Queue(maxsize=1000)
    
    # Counter for certs received per second (use a list to hold a mutable integer)
    cert_counter = [0]

    # Scheduling the coroutines
    await asyncio.gather(
        process_a(queue_ab, cert_counter), # Process A: Data intake
        process_b(queue_ab, queue_bc), # Process B: Data processing
        process_c(queue_bc), # Process C: Data storage and distribution
        process_d(queue_ab, queue_bc, cert_counter),  # Process D: Statistics display
    )

if __name__ == '__main__':
    # Running the asyncio event loop
    asyncio.run(main())
