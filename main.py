import asyncio
import time
from a_certs_firehose.a_certs_firehose import ACertsFirehose
from b_certs_filtering.b_certs_filtering import BCertsFiltering
from c_dns_multiplexer.c_dns_multiplexer import CDNSMultiplexer
from db_manager.db_manager import DBManager
from dotenv import load_dotenv

load_dotenv()

# Global database instance
db_manager = DBManager()
# Initialize the database connection once at startup
db_manager.init_connection()

# Tracking counters
cert_counter = [0]
filtered_counter = [0]
enriched_counter = [0]

# Process A: Certstream data intake
async def process_a(queue_ab, cert_counter):
    firehose = ACertsFirehose(queue_ab, cert_counter)
    await firehose.start_listening()

# Process B: Domains filtering
async def process_b(queue_ab, queue_bc):
    b_certs_filtering = BCertsFiltering(queue_bc)
    while True:
        all_domains = await queue_ab.get()  # Wait for next item in queue
        b_certs_filtering.filter(all_domains)
        # Increment the filtered domain counter
        filtered_counter[0] += len(all_domains)
        queue_ab.task_done()  # Mark item as processed

# Process C: Enriching domains with IPs and NS using CDNSMultiplexer
async def process_c(queue_bc, queue_cd, batch_size=4000):
    print("Starting process_c")
    c_dns_multiplexer = CDNSMultiplexer()
    while True:
        batch = {}
        
        # Collect exactly batch_size items from queue_bc
        for _ in range(batch_size):
            domains_and_ids = await queue_bc.get()  # Waits for each item
            batch.update(domains_and_ids)
            queue_bc.task_done()  # Mark item as processed

        # Start time for processing rate calculation
        start_time = time.time()

        # Process the batch once the exact batch size is reached
        await c_dns_multiplexer.enrich_domains(batch, queue_cd)
        
        # End time for processing rate calculation
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # Update enriched domain counter
        enriched_counter[0] += len(batch)
        
        # Optional debugging output
        print(f"Processed batch of size {len(batch)} in {elapsed_time:.2f} seconds.")

# Process D: Save final values (IPs and NS) to the database
async def process_d(queue_cd):
    while True:
        enriched_data = await queue_cd.get()  # Consume enriched data from queue_cd
        
        # Extract IP and NS data to prepare for batch database insertion
        ip_data = [(enriched_data["id"], ip) for ip in enriched_data["ips"]]
        ns_data = [(enriched_data["id"], ns) for ns in enriched_data["ns"]]

        # Insert IPs and NS records if they exist
        if ip_data:
            db_manager.insert_domains_ip(ip_data)
        if ns_data:
            db_manager.insert_domains_ns(ns_data)


# Process E: Display statistics for queue sizes and domain counts per second (5-minute rolling average)
async def process_e(queue_ab, queue_bc, queue_cd, cert_counter, lapse=1, rolling_window=300):
    # Initialize lists to store the last `rolling_window` seconds of data
    cert_history = []
    filtered_history = []
    enriched_history = []
    
    last_display_time = time.time()  # Initialize last display time
    
    while True:
        await asyncio.sleep(lapse)
        
        # Capture current counts for the past second
        certs_this_second = cert_counter[0]
        filtered_this_second = filtered_counter[0]
        enriched_this_second = enriched_counter[0]

        # Update history lists
        cert_history.append(certs_this_second)
        filtered_history.append(filtered_this_second)
        enriched_history.append(enriched_this_second)
        
        # Trim history lists to the rolling window length
        if len(cert_history) > rolling_window:
            cert_history.pop(0)
        if len(filtered_history) > rolling_window:
            filtered_history.pop(0)
        if len(enriched_history) > rolling_window:
            enriched_history.pop(0)
        
        # Calculate rolling averages
        certs_per_sec_avg = sum(cert_history) / rolling_window
        filtered_per_sec_avg = sum(filtered_history) / rolling_window
        enriched_per_sec_avg = sum(enriched_history) / rolling_window
        
        # Check if 5 minutes (300 seconds) have passed since last display
        if time.time() - last_display_time >= rolling_window:
            # Display queue sizes and average stats
            print("==========================================================")
            print(f"Queue AB size: {queue_ab.qsize()}, Queue BC size: {queue_bc.qsize()}, Queue CD size: {queue_cd.qsize()}")
            print("----------------------------------------------------------")
            print(f"Certs received per second ({(rolling_window/60):.0f}-min avg): {certs_per_sec_avg:.2f}")
            print(f"Domains filtered per second ({(rolling_window/60):.0f}-min avg): {filtered_per_sec_avg:.2f}")
            print(f"Domains enriched per second ({(rolling_window/60):.0f}-min avg): {enriched_per_sec_avg:.2f}")
            print("==========================================================")
            
            # Update the last display time
            last_display_time = time.time()
        
        # Reset counters for the next second
        cert_counter[0] = 0
        filtered_counter[0] = 0
        enriched_counter[0] = 0


async def main():
    # Initialize queues and counters
    queue_ab = asyncio.Queue(maxsize=1000)
    queue_bc = asyncio.Queue(maxsize=50000)
    queue_cd = asyncio.Queue(maxsize=1000)

    # Run all processes concurrently
    await asyncio.gather(
        process_a(queue_ab, cert_counter),
        process_b(queue_ab, queue_bc),
        process_c(queue_bc, queue_cd),
        process_d(queue_cd),
        process_e(queue_ab, queue_bc, queue_cd, cert_counter),
    )

if __name__ == '__main__':
    asyncio.run(main())
