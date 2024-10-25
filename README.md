
# Watchdog Intake Pipeline

This project is a high-performance, asynchronous pipeline designed to process and enrich domain certificates in real time. It uses a multi-stage queuing and processing architecture based on Apache Pulsar, DNS-over-HTTPS, and Python's asyncio library.

## Features

- **Certstream Intake**: Connects to Certstream to receive domain certificates in real time.
- **Domain Filtering**: Filters domains based on specified criteria before enrichment.
- **DNS Enrichment**: Enriches domains with IP and nameserver information using DNS-over-HTTPS requests.
- **Pulsar Integration**: Publishes enriched domain data to Apache Pulsar for further processing.
- **Database Integration**: Saves enriched IP and nameserver data to a MySQL database.

## Project Structure

- **a_certs_firehose/** - Handles certificate streaming and initial data intake.
- **b_certs_filtering/** - Filters and processes domain certificates.
- **c_dns_multiplexer/** - Asynchronously enriches domains with DNS information.
- **db_manager/** - Manages database connections and operations for saving IP and nameserver records.
- **pulsar/** - Manages connection to Apache Pulsar and handles message publishing.
- **main.py** - The main script to run the pipeline processes concurrently.

## Requirements

- Python 3.8+
- Apache Pulsar (client and broker)
- `aiohttp` for asynchronous HTTP requests
- `dotenv` for environment variable management
- `pymysql` for MySQL database operations

## Setup

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/ElBinaryNerd/watchdog_intake.git
   cd watchdog_intake
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment Variables**:

   Create a `.env` file in the project root with the following configuration:

   ```env
   # Pulsar Settings
   PULSAR_HOST=localhost
   PULSAR_PORT=6650
   PULSAR_TOPIC=domains-to-scrape

   # Database Settings
   DB_USER=username
   DB_PASSWORD=password
   DB_NAME=dbname
   DB_HOST=localhost
   DB_PORT=3306

   # Other Settings
   DOMAIN_TOPIC=your-domain-topic
   ```

## Usage

1. **Start Apache Pulsar** (if not already running):
   ```bash
   pulsar standalone
   ```

2. **Run the Pipeline**:
   ```bash
   python3 main.py
   ```

3. **Pipeline Statistics**:
   - The `process_e` function outputs statistics on domains processed per second across various stages, as well as the sizes of each processing queue.

## Code Overview

- **PulsarProducer**: Connects to the Pulsar broker and sends enriched domain data to a specified topic.
- **DBManager**: Manages MySQL database connections and saves IP and nameserver data.
- **CDNSMultiplexer**: Asynchronously resolves domain IPs and nameservers.
- **Main Processes**:
  - `process_a`: Certstream data intake.
  - `process_b`: Domain filtering.
  - `process_c`: DNS enrichment.
  - `process_d`: Data storage in the database.
  - `process_e`: Periodic logging of queue sizes and processing rates.

## License

This project is licensed under the MIT License.
