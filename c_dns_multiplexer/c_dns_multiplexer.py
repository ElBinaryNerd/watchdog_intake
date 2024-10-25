import aiohttp
import asyncio
import json
import re
import time

class CDNSMultiplexer:
    DOH_URL = "https://cloudflare-dns.com/dns-query"

    def __init__(self, semaphore_limit=500):
        # Limit concurrent DNS resolution requests
        self.semaphore = asyncio.Semaphore(semaphore_limit)
        self.session = None  # Will hold a reusable session for all requests

    async def init_session(self):
        """Initialize the aiohttp session for DNS queries."""
        self.session = aiohttp.ClientSession()

    async def close_session(self):
        """Close the aiohttp session."""
        if self.session:
            await self.session.close()

    async def async_dns_resolve(self, domain):
        """
        Resolves IP and NS for a given domain asynchronously using Cloudflare DoH.
        """
        try:
            ip_url = f"{self.DOH_URL}?name={domain}&type=A"
            ns_url = f"{self.DOH_URL}?name={domain}&type=NS"
            headers = {"accept": "application/dns-json"}

            async with self.semaphore:
                async with self.session.get(ip_url, headers=headers) as ip_response, \
                           self.session.get(ns_url, headers=headers) as ns_response:
                    
                    if ip_response.status == 200 and ns_response.status == 200:
                        ip_raw = await ip_response.text()
                        ns_raw = await ns_response.text()

                        # Parse IP response
                        ip_data = json.loads(ip_raw)
                        ips = [answer['data'] for answer in ip_data.get('Answer', []) if answer.get('type') == 1]

                        # Parse NS response to include records from both Answer and Authority sections
                        ns_data = json.loads(ns_raw)
                        
                        # Extract NS records from the response
                        all_nameservers = self.extract_nameservers(ns_data)
                        return ips, all_nameservers
                    else:
                        return [], []

        except Exception as e:
            print(f"DNS Resolution failed for domain {domain}: {e}")
            return [], []

    def extract_nameservers(self, ns_data):
        """Extract nameservers from NS and SOA records."""
        direct_nameservers = []
        authoritative_nameservers = []
        soa_nameservers = []

        ns_regex = re.compile(r"([a-zA-Z0-9-]+\.[a-zA-Z0-9.-]+\.)")

        for answer in ns_data.get('Answer', []):
            if answer.get('type') == 2:  # Direct NS type
                direct_nameservers.append(answer['data'])

        for authority in ns_data.get('Authority', []):
            if authority.get('type') == 2:  # NS type in Authority section
                authoritative_nameservers.append(authority['data'])
            elif authority.get('type') == 6:  # SOA type in Authority section
                soa_entries = ns_regex.findall(authority['data'])
                soa_nameservers.extend(soa_entries)

        return list(set(direct_nameservers + authoritative_nameservers + soa_nameservers))

    async def enrich_domains(self, domains_and_ids, queue_cd):
        """
        Resolves DNS for a batch of domains and places enriched results in queue_cd.
        """
        if not self.session:
            await self.init_session()

        batch_start_time = time.time()
        tasks = [self.process_and_enqueue(domain_id, domain, queue_cd) for domain, domain_id in domains_and_ids.items()]
        await asyncio.gather(*tasks)
        
        # Log batch processing time
        batch_end_time = time.time()
        print(f"Processed batch of size {len(domains_and_ids)} in {batch_end_time - batch_start_time:.2f} seconds")


    async def process_and_enqueue(self, domain_id, domain, queue_cd):
        """
        Processes a single domain to enrich with IP and NS data and enqueues the result in queue_cd.
        """
        ips, nameservers = await self.async_dns_resolve(domain)
        enriched_data = {
            "id": domain_id,
            "domain": domain,
            "ips": ips if ips else [],
            "ns": nameservers if nameservers else []
        }
        await queue_cd.put(enriched_data)
