import asyncio
from a_certs_firehose.a_certs_firehose import ACertsFirehose
from b_certs_filtering.b_certs_filtering import BCertsFiltering
from c_dns_multiplexer.c_dns_multiplexer import CDNSMultiplexer
from dotenv import load_dotenv

load_dotenv()

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
async def process_c(queue_bc, queue_cd, batch_size=800):
    c_dns_multiplexer = CDNSMultiplexer()
    while True:
        # Collect multiple items from queue_bc
        batch = {}
        for _ in range(batch_size):
            if not queue_bc.empty():
                domains_and_ids = await queue_bc.get()
                batch.update(domains_and_ids)
        
        if batch:
            print(f"Processing batch of size {len(batch)}")
            await c_dns_multiplexer.enrich_domains(batch, queue_cd)
        else:
            await asyncio.sleep(1)  # Slight pause if queue_bc is empty

# Process D: Save final value to a file
async def process_d(queue_cd):
    with open("output-asyncio.txt", "a") as f:
        while True:
            processed_value = await queue_cd.get()  # Consume data from queue
            f.write(f"Processed value: {processed_value}\n")
            f.flush()

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
    queue_bc = asyncio.Queue(maxsize=1000)
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
