# Confluent Cloud Prometheus Scrape Generator

A tool to generate Prometheus configuration for monitoring Confluent Cloud Kafka clusters.

## Overview

This tool automatically generates Prometheus scrape configurations for Confluent Cloud Kafka clusters. It:

- Fetches all Kafka clusters from your Confluent Cloud environments
- Generates proper labeling and metrics configurations
- Creates a Prometheus-compatible YAML configuration
- Supports Amazon Managed Prometheus (AMP) integration

## Prerequisites

- Python 3.x
- Confluent Cloud API credentials
- PyYAML and other Python dependencies

## Installation

```bash
# Clone the repository
git clone https://github.com/cjmatta/ccloud-prometheus-scrape-generator.git
cd ccloud-prometheus-scrape-generator

# Install dependencies
pip install -r requirements.txt
```

## Environment Setup

Set your Confluent Cloud credentials:

```bash
export CLOUD_API_KEY="your-api-key"
export CLOUD_API_SECRET="your-api-secret"
```

## Usage

1. Run the generator:

```bash
python ccloud-scrape-generator.py
```

The script will:
- Connect to Confluent Cloud API
- Fetch all available Kafka clusters
- Generate a `prometheus.yml` file with:
  - Scrape configurations
  - Cluster metadata labels
  - Environment information
  - Regional settings
  - Cluster type information


## Maintenance

Remember to regenerate the configuration when:
- Adding new Kafka clusters
- Changing environment settings
- Updating cluster configurations
- Modifying monitoring requirements

## Contributing

1. Fork the repository
2. Create your feature branch
3. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.