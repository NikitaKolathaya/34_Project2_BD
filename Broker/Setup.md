# Kafka Broker Node - Monisha (PES2UG23CS906)

**Role:** Kafka Broker
**Node IP:** `172.27.111.37`

---

## 🧠 Overview

This node is responsible for managing **Kafka message streaming**, including:

* Running **ZooKeeper** and **Kafka Server**
* Hosting and maintaining all Kafka topics
* Handling communication between producer and consumer nodes

---

## ⚙️ Setup Instructions

### 1. Start ZooKeeper

```bash
cd ~/kafka_2.13-3.6.1
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### 2. Start Kafka Server

```bash
cd ~/kafka_2.13-3.6.1
bin/kafka-server-start.sh config/server.properties
```

### 3. Update Configuration

Edit `~/kafka_2.13-3.6.1/config/server.properties`:

```
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://172.27.111.37:9092
```

Restart Kafka after saving.

---

## 🧾 Monitoring Commands

```bash
# List topics
bin/kafka-topics.sh --list --bootstrap-server 172.27.111.37:9092

# Describe topic
bin/kafka-topics.sh --describe --topic <topic_name> --bootstrap-server 172.27.111.37:9092

# Check consumer groups
bin/kafka-consumer-groups.sh --list --bootstrap-server 172.27.111.37:9092
```

---

## 🛠 Troubleshooting

| Issue                  | Fix                                                         |
| ---------------------- | ----------------------------------------------------------- |
| Broker not reachable   | Check `advertised.listeners` and firewall for port 9092     |
| Topic not found        | Verify admin approval and producer creation thread          |
| ZooKeeper not starting | Ensure `/tmp/zookeeper` directory is cleared before restart |

---

## 🧹 Cleanup

```bash
bin/kafka-server-stop.sh
bin/zookeeper-server-stop.sh
rm -rf /tmp/kafka-logs/ /tmp/zookeeper/
```

---

**Maintained by:**
**Monisha S (PES2UG23CS906)**
Kafka Broker Administrator
