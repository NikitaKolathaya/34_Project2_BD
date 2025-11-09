# Node 4 – Consumer (Nikita)
**IP:** 172.27.212.124  
**Role:** Kafka Consumer Node

Consumes real-time messages from the Kafka broker (`172.27.111.37:9092`)  
based on topics subscribed via the central database (`172.27.130.236`).

### Command to Run:
```bash
cd ~/bd_project
python3 consumer.py username

Supports viewing active topics, subscribing/unsubscribing dynamically,
and real-time message consumption with statistics and recent message logs.