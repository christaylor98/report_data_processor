#!/bin/bash
# Setup script for JSNL to Parquet processing pipeline

# Create necessary directories
mkdir -p /var/data/jsnl/processed
mkdir -p /var/data/parquet/temp
mkdir -p /var/data/parquet/hourly
mkdir -p /var/data/parquet/daily
mkdir -p /var/data/parquet/monthly
mkdir -p /var/log

# Create jsnl user
useradd -m jsnl
chown -R jsnl:jsnl /var/data/jsnl
chown -R jsnl:jsnl /var/data/parquet

# Install Python dependencies
pip3 install duckdb pyarrow mysql-connector-python

# Copy files to appropriate locations
cp jsnl_processor.py /usr/local/bin/
cp jsnl_recovery.sh /usr/local/bin/
chmod +x /usr/local/bin/jsnl_processor.py
chmod +x /usr/local/bin/jsnl_recovery.sh

# Install systemd service and timer
cp jsnl_processor.service /etc/systemd/system/
cp jsnl_processor.timer /etc/systemd/system/

# Enable and start the timer
systemctl daemon-reload
systemctl enable jsnl_processor.timer
systemctl start jsnl_processor.timer

echo "JSNL to Parquet processing pipeline installed successfully!"
echo "Check setup by running: systemctl status jsnl_processor.timer" 