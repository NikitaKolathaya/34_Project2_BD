# 34_Project2_BD
# Dynamic Content Stream with Kafka

### Team Members

| Name          | SRN           | Role                            | Node IP          |
| ------------- | ------------- | ------------------------------- | ---------------- |
| **Monisha S** | PES2UG23CS906 | **Kafka Broker**                | `172.27.111.37`  |
| **Nikita**    | PES2UG23CS387 | **Consumer**                    | `172.27.212.124` |
| **Nandana**   | PES2UG23CS913 | **Producer**                    | `172.27.101.142` |
| **Musharraf** | PES2UG23CS915 | **Admin + Frontend + Database** | `172.27.130.236` |

---

## 🧠 Project Overview

This project implements a **Dynamic Kafka-based content streaming system** that enables:

* Dynamic topic creation and approval
* Multi-threaded producer publishing
* Real-time subscription-based message consumption
* Centralized admin control with database and Flask frontend

Each participant represents a distributed node connected via the same network (ZeroTier or LAN).

---

## ⚙️ Architecture Overview

```
┌──────────────────────────────┐
│     Admin Panel (Musharraf)  │
│  • Flask + MySQL Database    │
│  • Approves Topics           │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────┴───────────────┐
│     Kafka Broker (Monisha)    │
│  • Runs Kafka + ZooKeeper     │
│  • Hosts all topics           │
└──────────────┬───────────────┘
               │
       ┌───────┴────────┐
       │                │
       ▼                ▼
┌──────────────┐   ┌──────────────┐
│ Producer      │   │ Consumer     │
│ (Nandana)     │   │ (Nikita)     │
│ Publishes msgs│   │ Subscribes to│
│ to approved   │   │ approved     │
│ topics        │   │ topics       │
└───────────────┘   └──────────────┘
```

---

## 🧩 Components

### 1. **Kafka Broker (Monisha)**

IP: `172.27.111.37`

Responsible for:

* Running **ZooKeeper** and **Kafka Server**
* Managing topic lifecycle
* Handling message streaming between producers and consumers

**Commands:**

```bash
# Start ZooKeeper
cd ~/kafka_2.13-3.6.1
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Broker
bin/kafka-server-start.sh config/server.properties
```

Ensure `server.properties` includes:

```
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://172.27.111.37:9092
```

---

### 2. **Admin Panel + Database (Musharraf)**

IP: `172.27.130.236`

Responsible for:

* Running **MySQL** and **Flask frontend**
* Managing topic requests and approvals
* Tracking user subscriptions

**Commands:**

```bash
sudo systemctl start mysql
cd ~/kafka-project
python3 admin.py   # Backend admin panel
python3 frontend.py  # Flask UI
```

**Access:**
Frontend: `http://172.27.130.236:5000`

Database:

* Host: `172.27.130.236`
* User: `kafka_user`
* DB: `kafka_streaming`

---

### 3. **Producer (Nandana)**

IP: `172.27.101.142`

Publishes messages to **approved topics** dynamically.
Runs three threads:

* **Input Listener Thread:** reads messages from user input
* **Publisher Thread:** sends queued messages to Kafka
* **Topic Watcher Thread:** auto-creates approved topics

**Command:**

```bash
cd ~/kafka-project
python3 producer.py
```

**Configuration (inside `producer.py`):**

```python
bootstrap_servers='172.27.111.37:9092'
db_host='172.27.130.236'
```

**Usage Example:**

```
news|Breaking: Major discovery in AI
sports|Team India wins championship
```

---

### 4. **Consumer (Nikita)**

IP: `172.27.212.124`

Consumes messages from topics subscribed via admin approval.
Supports interactive options for subscription management.

**Command:**

```bash
cd ~/kafka-project
python3 consumer.py pes2ug23cs387
```

**Configuration (inside `consumer.py`):**

```python
bootstrap_servers='172.27.111.37:9092'
db_host='172.27.130.236'
```

**Consumer Menu Options:**

1. View Active Topics
2. Subscribe to Topics
3. Unsubscribe from Topics
4. View Subscriptions
5. Start Consuming
6. Exit

---

## 🧪 Testing Workflow

### Step 1: Start Admin Panel (Musharraf)

```bash
python3 admin.py
python3 frontend.py
```

Create topic requests (e.g., `news`, `sports`, `tech`, `weather`).

### Step 2: Start Kafka Broker (Monisha)

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

### Step 3: Start Producer (Nandana)

```bash
python3 producer.py
```

Wait for admin topic approvals.

### Step 4: Approve Topics (Musharraf)

In Admin Panel:

* Approve topics via option 2.

### Step 5: Start Consumer (Nikita)

```bash
python3 consumer.py pes2ug23cs387
```

Subscribe to topics like `news` or `sports` and start consuming.

### Step 6: Publish Messages (Producer)

```bash
news|AI Conference 2025 announced
sports|New world record in sprinting
```

### Step 7: Verify Message Flow

Consumer displays only subscribed topics’ messages in real time.

---

## 🌐 Network Configuration (ZeroTier Recommended)

All nodes are connected under the same ZeroTier virtual network.
Each node uses its assigned IP for communication.

Ensure:

* Port `9092` open on Kafka broker
* Port `3306` open for MySQL
* Port `5000` open for Flask UI

---

## 🔍 Troubleshooting

| Issue                           | Fix                                                             |
| ------------------------------- | --------------------------------------------------------------- |
| Kafka not reachable             | Check broker IP & `advertised.listeners`                        |
| Consumer not receiving messages | Verify subscription in MySQL & topic existence                  |
| MySQL access error              | Ensure `bind-address=0.0.0.0` and proper GRANT permissions      |
| Flask UI not loading            | Check firewall and Flask port 5000 status                       |
| Topic not created               | Confirm admin approval and producer topic watcher thread active |

---

## 🧹 Cleanup Commands

```bash
# Stop services
bin/kafka-server-stop.sh
bin/zookeeper-server-stop.sh
sudo systemctl stop mysql

# Clear Kafka logs
rm -rf /tmp/kafka-logs/ /tmp/zookeeper/

# Reset database
mysql -u kafka_user -p kafka_streaming -e "TRUNCATE TABLE topics; TRUNCATE TABLE user_subscriptions;"
```

---

## ✅ Deliverables Checklist

| Component                             | Status |
| ------------------------------------- | ------ |
| Kafka setup & broker configuration    | ✅      |
| Multi-threaded producer               | ✅      |
| Dynamic topic approval via database   | ✅      |
| Interactive consumer menu             | ✅      |
| Flask admin dashboard                 | ✅      |
| Cross-node communication via ZeroTier | ✅      |

---

### Final Note

This distributed Kafka project demonstrates **dynamic message streaming and topic management** across multiple networked systems, emphasizing **asynchronous communication, multi-threading, and real-time coordination** between producer–broker–consumer pipelines.

**Developed by:**

> Monisha, Nikita, Nandana, Musharraf
> PES University (Batch of 2027)
