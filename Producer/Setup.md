🧩 Producer Setup Guide (p.py)

1️⃣ Purpose
This producer application connects to an Apache Kafka broker and:

Creates topics: weather, tech, sports, news, test

Publishes messages in JSON format containing "content", "timestamp", and "producer_id"

For tech topic — reads data line-by-line from tech.txt

For other topics — accepts live custom input

Automatically generates random messages every 20 seconds

2️⃣ Prerequisites

Ensure the following are installed and configured:

Component	Requirement

Python	Version 3.10+

Kafka	Broker running at 172.27.111.37:9092

MySQL (for consumer verification)	Host: 172.27.130.236

Libraries	kafka-python, mysql-connector-python

Install dependencies:
pip install kafka-python mysql-connector-python

3️⃣ Configuration


bootstrap_servers = '172.27.111.37:9092'
db_host = '172.27.130.236'
db_user = 'kafka_user'
db_password = '****'
db_name = 'kafka_streaming'
auto_stream = True

4️⃣ Required Files

File	Description
p.py	The producer script

tech.txt	Input file for tech topic (each line = one message)
