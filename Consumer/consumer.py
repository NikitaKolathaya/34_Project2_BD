#!/usr/bin/env python3

import threading
import time
import json
import mysql.connector
from kafka import KafkaConsumer
import sys
from datetime import datetime
from collections import defaultdict

#username
class DynamicKafkaConsumer:
    def __init__(self, user_id, bootstrap_servers='172.27.111.37:9092',
                 db_host='172.27.130.236', db_user='kafka_user',
                 db_password='Kafka@Pass123', db_name='kafka_streaming'):

        print(f" Initializing Consumer for User: {user_id}")

        self.user_id = user_id
        self.bootstrap_servers = bootstrap_servers

        # Database setup
        self.db_config = {
            'host': db_host,
            'user': db_user,
            'password': db_password,
            'database': db_name
        }

        #testing db 
        try:
            conn = self.get_db_connection()
            conn.close()
            print(f"✓ Connected to Database: {db_host}")
        except Exception as e:
            print(f" Failed to connect to database: {e}")
            sys.exit(1)

        #kafka consumer state
        self.consumer = None
        self.subscribed_topics = set()

        #thread control
        self.running = True        #whole program
        self.consuming = False
        self.consumer_lock = threading.Lock()  #preventing race
    #runtime
        self.messages_received = defaultdict(int)
        self.total_messages = 0
        self.processing_errors = 0
        self.last_message_time = None

        #message cache
        self.processed_messages = []
        self.max_stored_messages = 100

        
    def get_db_connection(self):
        return mysql.connector.connect(**self.db_config)

    def get_active_topics(self):
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT topic_name, created_at FROM topics WHERE status = 'active'")
            topics = cursor.fetchall()
            cursor.close()
            conn.close()
            return topics
        except Exception as e:
            print(f" Error fetching active topics: {e}")
            return []

    def get_user_subscriptions(self):
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT topic_name FROM user_subscriptions WHERE user_id = %s",
                (self.user_id,)
            )
            topics = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return topics
        except Exception as e:
            print(f" Error fetching subscriptions: {e}")
            return []

    def subscribe_to_topic(self, topic_name):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO user_subscriptions (user_id, topic_name) VALUES (%s, %s)",
                (self.user_id, topic_name)
            )
            conn.commit()
            print(f"✓ Subscribed to topic: {topic_name}")
            if self.consuming:
                self.update_consumer_subscriptions()
            return True
        except mysql.connector.IntegrityError:
            print(f" Already subscribed to: {topic_name}")
            return False
        except Exception as e:
            print(f" Error subscribing: {e}")
            return False
        finally:
            cursor.close()
            conn.close()

    def unsubscribe_from_topic(self, topic_name):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "DELETE FROM user_subscriptions WHERE user_id = %s AND topic_name = %s",
                (self.user_id, topic_name)
            )
            conn.commit()
            if cursor.rowcount > 0:
                print(f"✓ Unsubscribed from topic: {topic_name}")
                if self.consuming:
                    self.update_consumer_subscriptions()
            else:
                print(f" Not subscribed to: {topic_name}")
        except Exception as e:
            print(f" Error unsubscribing: {e}")
        finally:
            cursor.close()
            conn.close()

    
    def initialize_consumer(self):
        #creates kafkaconsumer
        with self.consumer_lock:
            subscribed_topics = self.get_user_subscriptions()
            if not subscribed_topics:
                print(" No subscriptions found. Please subscribe first.")
                return False

            try:
                if self.consumer:
                    self.consumer.close()

                self.consumer = KafkaConsumer(
                    *subscribed_topics,
                    bootstrap_servers=self.bootstrap_servers,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    group_id=f'consumer-group-{self.user_id}',
                    session_timeout_ms=10000,
                    heartbeat_interval_ms=3000
                )

                self.subscribed_topics = set(subscribed_topics)
                print(f"✓ Consumer initialized with topics: {subscribed_topics}")
                return True
            except Exception as e:
                print(f" Failed to initialize consumer: {e}")
                return False

    def update_consumer_subscriptions(self):
        #unlimited subs
        with self.consumer_lock:
            current_subscriptions = set(self.get_user_subscriptions())

            if current_subscriptions != self.subscribed_topics:
                added = current_subscriptions - self.subscribed_topics
                removed = self.subscribed_topics - current_subscriptions

                if added:
                    print(f" Adding new topics: {added}")
                if removed:
                    print(f" Removing topics: {removed}")

                try:
                    # Fully reinitialize the consumer to force proper rebalancing
                    if self.consumer:
                        try:
                            self.consumer.close()
                            print(" Old consumer closed for re-subscription")
                        except Exception as e:
                            print(f" Error closing old consumer: {e}")

                    self.consumer = KafkaConsumer(
                        *list(current_subscriptions),
                        bootstrap_servers=self.bootstrap_servers,
                        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                        auto_offset_reset='latest',
                        enable_auto_commit=True,
                        group_id=f'consumer-group-{self.user_id}',
                        session_timeout_ms=10000,
                        heartbeat_interval_ms=3000
                    )

                    self.subscribed_topics = current_subscriptions
                    print(f" Now subscribed to topics: {list(self.subscribed_topics)}")

                except Exception as e:
                    print(f" Error updating subscriptions: {e}")

    
    def process_message(self, message):
        try:
            topic = message.topic
            timestamp = datetime.fromtimestamp(message.timestamp / 1000)
            value = message.value

            if isinstance(value, dict):
                content = value.get("content", str(value))
                message_timestamp = value.get("timestamp", time.time())
            else:
                content = str(value)
                message_timestamp = time.time()

            latency = max(time.time() - message_timestamp, 0)
            self.messages_received[topic] += 1
            self.total_messages += 1
            self.last_message_time = time.time()

            self.processed_messages.append({
                "topic": topic,
                "timestamp": timestamp,
                "content": content,
                "latency": latency
            })
            if len(self.processed_messages) > self.max_stored_messages:
                self.processed_messages.pop(0)

            print(f"\n[{self.total_messages}] {topic} | {latency:.3f}s | {content[:60]}")
        except Exception as e:
            self.processing_errors += 1
            print(f" Error processing message: {e}")

    def consume_messages_thread(self):
        print(" Listening for messages...")
        while self.consuming:
            try:
                messages = self.consumer.poll(timeout_ms=100, max_records=100)
                for _, records in messages.items():
                    for record in records:
                        self.process_message(record)
            except Exception as e:
                print(f" Consumer error: {e}")
                time.sleep(1)
        print("✓ Consumer thread stopped")

    def subscription_watcher_thread(self):
        #subscription changes
        print(" Subscription watcher started")
        while self.consuming:
            try:
                time.sleep(2)
                self.update_consumer_subscriptions()
            except Exception as e:
                print(f" Watcher error: {e}")
        print("✓ Subscription watcher stopped")

 
    def print_statistics(self):
        print("\n" + "=" * 70)
        print(f" CONSUMER STATISTICS - User: {self.user_id}")
        print("=" * 70)
        print(f"Total Messages Received: {self.total_messages}")
        print(f"Processing Errors: {self.processing_errors}")
        print(f"Active Subscriptions: {len(self.subscribed_topics)}")
        print(f"Last Message: {time.ctime(self.last_message_time) if self.last_message_time else 'None'}")
        print("=" * 70)

    def show_recent_messages(self, count=10):
        print("\n" + "=" * 70)
        print(f" RECENT MESSAGES (Last {count})")
        print("=" * 70)
        for m in self.processed_messages[-count:]:
            print(f"[{m['timestamp']}] {m['topic']}: {m['content']} (latency={m['latency']:.3f}s)")
        print("=" * 70)

    def interactive_menu(self):
        print("\n" + "=" * 70)
        print(f"KAFKA CONSUMER - User: {self.user_id}")
        print("=" * 70)

        while self.running:
            print("\n1. View Active Topics")
            print("2. Subscribe to Topic(s)")
            print("3. Unsubscribe from Topic")
            print("4. View My Subscriptions")
            print("5. Start Consuming Messages")
            print("6. Stop Consuming")
            print("7. View Statistics")
            print("8. View Recent Messages")
            print("9. Exit")

            choice = input("\nEnter your choice: ").strip()

            if choice == "1":
                topics = self.get_active_topics()
                print("\nACTIVE TOPICS")
                for t in topics:
                    print(f"• {t['topic_name']} (Created: {t['created_at']})")

            elif choice == "2":
                topics = self.get_active_topics()
                if not topics:
                    print("No active topics.")
                    continue
                print("\nSelect multiple topics (e.g., 1,2,3):")
                for i, t in enumerate(topics, 1):
                    print(f"{i}. {t['topic_name']}")
                try:
                    indices = [int(x) - 1 for x in input("\nEnter topic number(s): ").split(',')]
                    for i in indices:
                        if 0 <= i < len(topics):
                            self.subscribe_to_topic(topics[i]['topic_name'])
                except ValueError:
                    print("Invalid input.")

            elif choice == "3":
                subs = self.get_user_subscriptions()
                if not subs:
                    print("No subscriptions found.")
                    continue
                for i, s in enumerate(subs, 1):
                    print(f"{i}. {s}")
                try:
                    idx = int(input("Enter topic number to unsubscribe: ")) - 1
                    if 0 <= idx < len(subs):
                        self.unsubscribe_from_topic(subs[idx])
                except ValueError:
                    print("Invalid input.")

            elif choice == "4":
                subs = self.get_user_subscriptions()
                print("\nYOUR SUBSCRIPTIONS:")
                for s in subs:
                    print(f"• {s}")

            elif choice == "5":
                if not self.consuming:
                    if self.initialize_consumer():
                        self.consuming = True
                        threading.Thread(target=self.consume_messages_thread, daemon=True).start()
                        threading.Thread(target=self.subscription_watcher_thread, daemon=True).start()
                else:
                    print(" Already consuming.")

            elif choice == "6":
                self.consuming = False
                print("✓ Consumer stopped")

            elif choice == "7":
                self.print_statistics()

            elif choice == "8":
                self.show_recent_messages()

            elif choice == "9":
                print("Exiting...")
                self.running = False
                self.consuming = False
                if self.consumer:
                    self.consumer.close()
                break

            else:
                print("Invalid choice. Try again.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        user_id = input("Enter your User ID: ").strip()
    else:
        user_id = sys.argv[1]

    consumer = DynamicKafkaConsumer(user_id)
    try:
        consumer.interactive_menu()
    except KeyboardInterrupt:
        print("\n Shutting down consumer...")
        consumer.running = False
        consumer.consuming = False
        if consumer.consumer:
            consumer.consumer.close()
    finally:
        print("✓ Consumer shutdown complete")