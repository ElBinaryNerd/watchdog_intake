# Domain Processing Pipeline with Pulsar and DNS Multiplexing

This project is a high-performance, asynchronous pipeline for processing and enriching domain certificates with IPs and nameservers. It leverages multiple stages with message queuing and distributed processing using Apache Pulsar, DNS-over-HTTPS, and Python's asyncio.

## Features

- **Certstream Intake**: Connects to Certstream to receive domain certificates in real time.
- **Filtering**: Filters domains based on criteria before enrichment.
- **DNS Enrichment**: Enriches domains with IP and NS information using DNS-over-HTTPS requests.
- **Pulsar Integration**: Publishes enriched domain data to Apache Pulsar for further processing.
- **Database Integration**: Saves enriched IP and NS data to a MySQL database.

## Project Structure

- **a_certs_firehose/** - Handles certificate streaming and initial data intake.
- **b_certs_filtering/** - Filters and processes domain certificates.
- **c_dns_multiplexer/** - Asynchronously enriches domains with DNS information.
- **db_manager/** - Manages database connections and operations for saving IP and NS records.
- **pulsar/** - Manages connection to Apache Pulsar and handles message publishing.
- **main.py** - The main script to run the pipeline processes concurrently.

## Requirements

- Python 3.8+
- `pulsar-client` library for Pulsar integration
- `aiohttp` for asynchronous HTTP requests
- `dotenv` for environment variable management
- `pymysql` for MySQL database operations

## Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/ElBinaryNerd/watchdog_intake.git
   cd watchdog_intake
