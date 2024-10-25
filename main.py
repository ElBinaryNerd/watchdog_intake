import asyncio
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

# Process C: Enriching domains with IPs and NS using CDNSMultiplexer
async def process_c(queue_bc, queue_cd, batch_size=4000):
    c_dns_multiplexer = CDNSMultiplexer()
    while True:
        batch = {}

        # Collect exactly batch_size items from queue_bc
        for _ in range(batch_size):
            domains_and_ids = await queue_bc.get()  # Waits for each item, blocking until it’s available
            batch.update(domains_and_ids)
            queue_bc.task_done()  # Mark item as processed for queue size tracking

        # Process the batch once the exact batch size is reached
        print(f"Processing batch of size {len(batch)}")
        await c_dns_multiplexer.enrich_domains(batch, queue_cd)

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

# Process E: Display queue sizes and certs received per second
async def process_e(queue_ab, queue_bc, queue_cd, cert_counter):
    while True:
        await asyncio.sleep(1)
        print(f"Queue AB size: {queue_ab.qsize()}, Queue BC size: {queue_bc.qsize()}, Queue CD size: {queue_cd.qsize()}")
        print(f"Certs received in the last second: {cert_counter[0]}")
        cert_counter[0] = 0  # Reset counter every second

async def main():
    # Initialize queues and counters
    queue_ab = asyncio.Queue(maxsize=1000)
    queue_bc = asyncio.Queue(maxsize=50000)
    queue_cd = asyncio.Queue(maxsize=1000)
    cert_counter = [0]

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
