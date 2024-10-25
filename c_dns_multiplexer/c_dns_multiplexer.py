import aiohttp
import asyncio
import json

class CDNSMultiplexer:
    DOH_URL = "https://cloudflare-dns.com/dns-query"

    def __init__(self, semaphore_limit=500):
        # Limit concurrent DNS resolution requests to avoid overwhelming the DNS server
        self.semaphore = asyncio.Semaphore(semaphore_limit)

    async def async_dns_resolve(self, domain, session):
        """
        Resolves IP and NS for a given domain asynchronously using Cloudflare DoH.
        """
        try:
            ip_url = f"{self.DOH_URL}?name={domain}&type=A"
            ns_url = f"{self.DOH_URL}?name={domain}&type=NS"
            headers = {"accept": "application/dns-json"}

            async with session.get(ip_url, headers=headers) as ip_response, session.get(ns_url, headers=headers) as ns_response:
                if ip_response.status == 200 and ns_response.status == 200:
                    ip_raw = await ip_response.text()
                    ns_raw = await ns_response.text()

                    # Parse IP response
                    ip_data = json.loads(ip_raw)
                    ips = [answer['data'] for answer in ip_data.get('Answer', []) if answer.get('type') == 1]

                    # Parse NS response
                    ns_data = json.loads(ns_raw)
                    nameservers = [answer['data'] for answer in ns_data.get('Answer', []) if answer.get('type') == 2]

                    return ips, nameservers
                else:
                    return None, None

        except Exception as e:
            print(f"DNS Resolution failed for domain {domain}: {e}")
            return None, None

    async def enrich_domains(self, domains_and_ids, queue_cd):
        """
        Resolves DNS for a batch of domains and places enriched results in queue_cd.
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            for domain, domain_id in domains_and_ids.items():
                # Each task enriches a domain and pushes the result to queue_cd
                tasks.append(self.process_and_enqueue(domain_id, domain, session, queue_cd))

            await asyncio.gather(*tasks)

    async def process_and_enqueue(self, domain_id, domain, session, queue_cd):
        """
        Processes a single domain to enrich with IP and NS data and enqueues the result in queue_cd.
        """
        async with self.semaphore:
            ips, nameservers = await self.async_dns_resolve(domain, session)
            enriched_data = {
                "id": domain_id,
                "domain": domain,
                "ips": ips if ips else [],
                "ns": nameservers if nameservers else []
            }
            print(enriched_data)
            await queue_cd.put(enriched_data)
