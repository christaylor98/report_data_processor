# JSNL to Parquet Processing Pipeline

A robust data processing pipeline that converts JSNL files into Parquet format using DuckDB, with additional functionality to extract trade and equity data to MariaDB.

## Overview

This project provides a data processing pipeline that:

- Processes JSNL files into Parquet format
- Extracts trade and equity data to MariaDB
- Supports hourly, daily, and monthly data aggregation
- Includes automated processing via systemd timer
- Provides flexible command-line interface for processing

## Prerequisites

- Python 3.x
- MariaDB/MySQL
- DuckDB
- Systemd (for automated processing)

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd report_data_processor
```

2. Run the setup script:

```bash
sudo ./setup.sh
```

The setup script will:
- Create necessary directories
- Install required Python dependencies
- Set up systemd service and timer
- Configure the processing pipeline

## Configuration

The pipeline uses environment variables for database configuration. Create a `.env` file with the following variables:

```
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=trading
```

## Usage

### Command Line Interface

The processor can be run in several ways:

1. Process all files in default directories:
```bash
python jsnl_processor.py
```

2. Process a specific file:
```bash
python jsnl_processor.py --file /path/to/file.jsnl
```

3. Process files from custom directories:
```bash
python jsnl_processor.py --source_path /custom/input --processed_path /custom/processed
```

4. Limit processing to N files:
```bash
python jsnl_processor.py --limit 10
```

### Automated Processing

The pipeline is configured to run automatically via systemd timer. To check the status:

```bash
systemctl status jsnl_processor.timer
```

## Directory Structure

- `/data/to_process/dashboard_data_archive` - Input directory for JSNL files
- `/data2/processed/dashboard_data_archive` - Processed JSNL files
- `/data/parquet/` - Output directory for Parquet files
  - `/temp` - Temporary processing files
  - `/hourly` - Hourly aggregated data
  - `/daily` - Daily aggregated data
  - `/monthly` - Monthly aggregated data
- `/log` - Log files

## Logging

Logs are written to `/log/jsnl_processor.log` with detailed information about the processing pipeline's operation.

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

[Add your license information here]

## Support

[Add support information here]
