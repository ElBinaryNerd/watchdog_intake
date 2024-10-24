import asyncio
import tldextract
from dictionary.skippable_subdomains import get_skippable
from dictionary.domain_tld import get_tld_blacklist
from db_manager.db_manager import DBManager

class BCertsFiltering:
    def __init__(self, queue_ab, queue_bc, db_manager):
        self.queue_ab = queue_ab
        self.queue_bc = queue_bc
        self.db_manager = DBManager()
        self.loop = asyncio.get_running_loop()

    def filter(self, domains_to_filter):
        domains_filtered = self._filter_multidomains(domains_to_filter)
        domains_filtered = self._filter_restricted_tlds(domains_filtered)
        domains_filtered = self._filter_wildcard_and_duplicates(domains_filtered)
        # inserted_domains_ids = self._filter_duplicates(domains_filtered)
        self.queue_bc.put_nowait(domains_filtered)

    # Multi-level subdomain filter
    def _filter_multidomains(self, domains_in):
        domains_out = []
        for domain in domains_in:
            subdomain_parts = tldextract.extract(domain).subdomain.split('.')
            if len(subdomain_parts) <= 1:
                domains_out.append(domain)
        return domains_out

    # Restricted TLDs filter
    def _filter_restricted_tlds(self, domains_in):
        domains_out = []
        skippable_tlds = get_tld_blacklist()
        for domain in domains_in:
            tld_part = tldextract.extract(domain).suffix.lower()
            if tld_part not in skippable_tlds:
                domains_out.append(domain)
        return domains_out
    
    def _filter_wildcard_and_duplicates(self, domains_in):
        """
        Filter out domains starting with '*.' and remove duplicates.
        Returns the list of cleaned domains and the count of filtered items.
        """
        unique_domains = set()
        for domain in domains_in:
            if domain.startswith('*.'):
                domain = domain[2:]
            if domain.startswith('www.'):
                domain = domain[4:]
            unique_domains.add(domain)
        return list(unique_domains)

    # Service-based subdomains filter
    def _filter_service_based_subdomains(self, domains_in):
        domains_out = []
        skippable_subdomains = get_skippable()
        for domain in domains_in:
            subdomain_part = tldextract.extract(domain).subdomain.lower()
            if subdomain_part not in skippable_subdomains:
                domains_out.append(domain)
        return domains_out

    # Filter duplicates via database (synchronously now)
    def _filter_duplicates(self, domains_in):
        """
        Check duplicates by querying the database synchronously.
        """
        inserted_domains_ids = self.db_manager.insert_non_duplicates(domains_in)
        return inserted_domains_ids