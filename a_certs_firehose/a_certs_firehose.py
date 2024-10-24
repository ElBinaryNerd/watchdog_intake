"""
This class aims to provide certificates in a non-blocking manner
The filtering by certificate validity time has been moved to this
class, as it does not require to iterate through the individuals
domains, and allows to reduce the overhead on the queue_ab.
"""

import asyncio
import certstream
from dotenv import load_dotenv
import os

# Load environment variables from the .env file at the root of the app
load_dotenv()

class ACertsFirehose:
    def __init__(self, queue_ab, cert_counter):
        self.cert_max_validity = int(os.getenv("CERT_MAX_VALIDITY"))
        self.queue_ab = queue_ab
        self.cert_counter = cert_counter
        self.event_count = 0
        self.loop = asyncio.get_running_loop()

    def callback(self, message, context):
        """
        Synchronous callback function that processes certstream events
        and pushes relevant certificate data to the asyncio queue.
        """
        self.event_count += 1
        self.cert_counter[0] += 1  # Increment cert counter

        if self.event_count % 200 == 0:  # Limit data flow to prevent overflow during testing
            leaf_cert = message['data'].get('leaf_cert', {})
            not_before = leaf_cert.get('not_before')
            not_after = leaf_cert.get('not_after')

            # Process only those with the required validity time
            if (not_after - not_before) < self.cert_max_validity:
                print(f"Certstream event {self.event_count}")
                self.queue_ab.put_nowait(leaf_cert.get('all_domains', []))

    async def start_listening(self):
        """
        Start the certstream listener in a separate thread to avoid blocking.
        """
        await asyncio.to_thread(certstream.listen_for_events, self.callback, url='wss://certstream.calidog.io/')
