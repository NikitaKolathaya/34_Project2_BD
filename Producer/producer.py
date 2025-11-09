import threading
import queue
import time
import json
import mysql.connector
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
import sys
import os
import random
from datetime import datetime

filepath="tech.txt"
class StreamingDataGenerator:
    """Generates realistic streaming data for different topics"""
    
    def __init__(self):
        # News headlines
        self.news_templates = [
            "Stock market {action} by {percent}% amid {reason}",
            "Breaking: {event} announced in {location}",
            "{company} reports {metric} earnings for Q{quarter}",
            "Government announces new {policy} policy",
            "International summit on {topic} concludes",
        ]
        
        # Sports updates
        self.sports_templates = [
            "{team1} defeats {team2} {score1}-{score2} in {event}",
            "{player} scores {points} points in comeback victory",
            "{team} advances to {stage} after dramatic win",
            "Championship game scheduled: {team1} vs {team2}",
            "{sport} season kicks off with surprise results",
        ]
        
        # Tech news
        self.tech_templates = [
            "{company} launches new {product} with {feature}",
            "AI breakthrough in {field} announced by researchers",
            "{tech} adoption grows {percent}% year-over-year",
            "Security vulnerability discovered in {software}",
            "New {device} pre-orders exceed expectations",
        ]
        
        # Weather updates
        self.weather_templates = [
            "{condition} expected in {location} with {temp}°F high",
            "{warning} issued for {region} through {day}",
            "Temperature to reach {temp}°F in {city} today",
            "{season} storm system moving through {area}",
            "Clear skies and {temp}°F forecasted for {location}",
        ]
        
        # Data for templates
        self.data = {
            'actions': ['rises', 'falls', 'surges', 'drops', 'stabilizes'],
            'companies': ['Tesla', 'Apple', 'Google', 'Microsoft', 'Amazon', 'Meta'],
            'events': ['merger', 'product launch', 'policy change', 'acquisition'],
            'locations': ['New York', 'London', 'Tokyo', 'Singapore', 'Dubai'],
            'teams': ['Lakers', 'Warriors', 'Heat', 'Celtics', 'Bulls', 'Nets'],
            'players': ['LeBron', 'Curry', 'Durant', 'Giannis', 'Jokic'],
            'sports': ['Basketball', 'Football', 'Baseball', 'Soccer'],
            'products': ['smartphone', 'laptop', 'AI model', 'cloud service'],
            'devices': ['smartphone', 'smartwatch', 'tablet', 'earbuds'],
            'conditions': ['Sunny', 'Cloudy', 'Rainy', 'Stormy', 'Foggy'],
            'warnings': ['Storm warning', 'Heat advisory', 'Flood watch', 'Wind alert'],
            'cities': ['Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Miami'],
        }
    
    def generate_message(self, topic_name):
        """Generate realistic message for a topic"""
        if 'news' in topic_name.lower():
            template = random.choice(self.news_templates)
            return template.format(
                action=random.choice(self.data['actions']),
                percent=random.randint(1, 15),
                reason=random.choice(['economic data', 'policy changes', 'market trends']),
                event=random.choice(self.data['events']),
                location=random.choice(self.data['locations']),
                company=random.choice(self.data['companies']),
                metric='strong' if random.random() > 0.5 else 'weak',
                quarter=random.randint(1, 4),
                policy=random.choice(['tax', 'trade', 'energy', 'healthcare']),
                topic=random.choice(['climate', 'technology', 'trade', 'security'])
            )
        
        elif 'sport' in topic_name.lower():
            template = random.choice(self.sports_templates)
            teams = random.sample(self.data['teams'], 2)
            return template.format(
                team1=teams[0], team2=teams[1],
                team=random.choice(self.data['teams']),
                score1=random.randint(80, 120),
                score2=random.randint(80, 120),
                event=random.choice(['finals', 'playoffs', 'championship']),
                player=random.choice(self.data['players']),
                points=random.randint(20, 50),
                stage=random.choice(['finals', 'semifinals', 'quarterfinals']),
                sport=random.choice(self.data['sports'])
            )
        
        elif 'tech' in topic_name.lower():
            template = random.choice(self.tech_templates)
            return template.format(
                company=random.choice(self.data['companies']),
                product=random.choice(self.data['products']),
                feature=random.choice(['AI capabilities', '5G support', 'faster performance']),
                field=random.choice(['natural language', 'computer vision', 'robotics']),
                tech=random.choice(['Cloud computing', 'AI', 'Blockchain', '5G']),
                percent=random.randint(10, 50),
                software=random.choice(['browser', 'operating system', 'database']),
                device=random.choice(self.data['devices'])
            )
        
        elif 'weather' in topic_name.lower():
            template = random.choice(self.weather_templates)
            return template.format(
                condition=random.choice(self.data['conditions']),
                location=random.choice(self.data['locations']),
                temp=random.randint(60, 95),
                warning=random.choice(self.data['warnings']),
                region=random.choice(['coastal areas', 'mountain regions', 'valleys']),
                day=random.choice(['Monday', 'Tuesday', 'Wednesday']),
                city=random.choice(self.data['cities']),
                season=random.choice(['Winter', 'Summer', 'Spring', 'Fall']),
                area=random.choice(self.data['locations'])
            )
        
        else:
            # Generic message for unknown topics
            return f"Data update for {topic_name} at {datetime.now().strftime('%H:%M:%S')}"


class DynamicKafkaProducer:
    def __init__(self, bootstrap_servers='172.27.111.37:9092', 
                 db_host='172.27.130.236', db_user='kafka_user', 
                 db_password='Kafka@Pass123', db_name='kafka_streaming',
                 auto_stream=True):
        
        print("------- Initializing Dynamic Kafka Producer...")
        
        self.bootstrap_servers = bootstrap_servers
        self.auto_stream = auto_stream  # NEW: Enable auto-streaming
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print(f"✓ Connected to Kafka Broker: {bootstrap_servers}")
        except Exception as e:
            print(f"XXXXXXX Failed to connect to Kafka: {e}")
            sys.exit(1)
        
        try:
            self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            print("✓ Kafka Admin Client initialized")
        except Exception as e:
            print(f"XXXXXXX Failed to initialize Admin Client: {e}")
            sys.exit(1)
        
        self.db_config = {
            'host': db_host,
            'user': db_user,
            'password': db_password,
            'database': db_name
        }
        
        try:
            conn = self.get_connection()
            conn.close()
            print(f"✓ Connected to Database: {db_host}")
        except Exception as e:
            print(f"XXXXXXX Failed to connect to database: {e}")
            sys.exit(1)
        
        # Message queue
        self.message_queue = queue.Queue()
        
        # Topic tracking
        self.created_topics = set()
        self.known_topics = set()
        
        # Threading control
        self.running = True
        
        # Statistics
        self.messages_published = 0
        self.messages_queued = 0
        self.messages_auto_generated = 0
        
        # Data generator
        self.data_generator = StreamingDataGenerator()
        
        # Streaming configuration
        self.stream_interval = 3  # Generate message every 3 seconds
        self.stream_batch_size = 1  # Messages per interval
    
    def get_connection(self):
        return mysql.connector.connect(**self.db_config)
    def publisher_thread(self):
        """Publishes data to correct topics in real time"""
        print("✓ Publisher Thread Started")
        error_count = 0
        
        while self.running:
            try:
                message_data = self.message_queue.get(timeout=1)
                
                topic_name = message_data['topic']
                message = message_data['message']
                
                if not self.verify_topic_active(topic_name):
                    print(f"⚠ Topic {topic_name} is not active. Message skipped.")
                    continue
                
                future = self.producer.send(topic_name, message)
                record_metadata = future.get(timeout=10)
                self.producer.flush()
                self.messages_published += 1
                
                source = "🤖 AUTO" if message.get('auto_generated') else "👤 MANUAL"
                print(f"📤 {source} → {topic_name} [P:{record_metadata.partition}, O:{record_metadata.offset}]")
                print(f"   {message['content'][:80]}...")
                
                error_count = 0
                
            except queue.Empty:
                continue
            except Exception as e:
                error_count += 1
                print(f"XXXXXXX Publisher error: {e}")
                if error_count > 5:
                    time.sleep(10)
                    error_count = 0
    
    def verify_topic_active(self, topic_name):
        """Verify topic exists and is active in database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT status FROM topics WHERE topic_name = %s AND status = 'active'",
                (topic_name,)
            )
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            return result is not None
        except Exception as e:
            return False
    

    def auto_streaming_thread(self):
        """Continuously generates and streams data to active topics"""
        print("✓ Auto-Streaming Thread Started")
        print(f"  Generating data every {self.stream_interval} seconds")
        
        while self.running:
            try:
                if not self.auto_stream:
                    time.sleep(10)
                    continue
                
                # Get all active topics
                active_topics = self.get_active_topics_list()
                
                if not active_topics:
                    time.sleep(5)
                    continue
                
                # Generate messages for random active topics
                for _ in range(self.stream_batch_size):
                    topic = random.choice(active_topics)
                    content = self.data_generator.generate_message(topic)
                    
                    message = {
                        'content': content,
                        'timestamp': time.time(),
                        'producer_id': 'auto_streaming_producer',
                        'auto_generated': True
                    }
                    
                    self.message_queue.put({
                        'topic': topic,
                        'message': message
                    })
                    
                    self.messages_queued += 1
                    self.messages_auto_generated += 1
                
                # Wait before next batch
                time.sleep(self.stream_interval)
                
            except Exception as e:
                print(f"XXXXXXX Auto-streaming error: {e}")
                time.sleep(5)
    
    def get_active_topics_list(self):
        """Get list of active topics for streaming"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT topic_name FROM topics WHERE status = 'active'")
            topics = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return topics
        except Exception as e:
            return []
#THREAD
    def input_listener_thread(self):
        """Accepts live/file input and pushes to publisher queue"""
        print("✓ Input Listener Thread Started")
        print("\n" + "="*60)
        print("INPUT MODES:")
        print("1. Live Input: topic_name|message_content")
        print("2. File Input: file:path/to/file.txt")
        print("3. Batch Input: batch:topic_name|msg1;msg2;msg3")
        print("\nCOMMANDS:")
        print("  stats       - Show statistics")
        print("  topics      - Show active topics")
        print("  auto on/off - Toggle auto-streaming")
        print("  speed <N>   - Set streaming interval (seconds)")
        print("  exit        - Shutdown producer")
        print("="*60 + "\n")
        
        while self.running:
            try:
                user_input = input(">> ").strip()
                
                if not user_input:
                    continue
                
                # Handle commands
                if user_input.lower() == 'exit':
                    print("🛑 Shutting down producer...")
                    self.running = False
                    break
                
                elif user_input.lower() == 'stats':
                    self.print_statistics()
                    continue
                
                elif user_input.lower() == 'topics':
                    self.print_active_topics()
                    continue
                
                elif user_input.lower().startswith('auto '):
                    state = user_input.split()[1].lower()
                    if state == 'on':
                        self.auto_stream = True
                        print("✓ Auto-streaming ENABLED")
                    elif state == 'off':
                        self.auto_stream = False
                        print("✓ Auto-streaming DISABLED")
                    continue
                
                elif user_input.lower().startswith('speed '):
                    try:
                        interval = int(user_input.split()[1])
                        self.stream_interval = interval
                        print(f"✓ Streaming interval set to {interval} seconds")
                    except:
                        print("XXXXXXX Invalid speed. Usage: speed <seconds>")
                    continue
                
                # Handle file input
                elif user_input.startswith('file:'):
                    self.process_file_input(user_input[5:])
                    continue
                
                # Handle batch input
                elif user_input.startswith('batch:'):
                    self.process_batch_input(user_input[6:])
                    continue
                
                # Handle live input
                else:
                    self.process_live_input(user_input)
                
            except EOFError:
                break
            except KeyboardInterrupt:
                print("\n⚠ Interrupt received")
                self.running = False
                break
            except Exception as e:
                print(f"XXXXXXX Input listener error: {e}")
    
    def process_live_input(self, user_input):
        """Process single live input"""
        parts = user_input.split('|', 1)
        if len(parts) != 2:
            print("⚠ Invalid format. Use: topic_name|message_content")
            return
        
        topic_name, message_content = parts
        topic_name = topic_name.strip()
        message_content = message_content.strip()
        
        message = {
            'content': message_content,
            'timestamp': time.time(),
            'producer_id': 'manual_producer',
            'auto_generated': False
        }
        
        self.message_queue.put({
            'topic': topic_name,
            'message': message
        })
        
        self.messages_queued += 1
        print(f"✓ Manual message queued for: {topic_name}")
    
    def process_file_input(self, filepath):
        """Process input from file"""
        try:
            if not os.path.exists(filepath):
                print(f"XXXXXXX File not found: {filepath}")
                return
            
            with open(filepath, 'r') as f:
                lines = f.readlines()
            
            count = 0
            for line in lines:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                parts = line.split('|', 1)
                if len(parts) == 2:
                    topic_name, message_content = parts
                    
                    message = {
                        'content': message_content.strip(),
                        'timestamp': time.time(),
                        'producer_id': 'file_producer',
                        'auto_generated': False
                    }
                    
                    self.message_queue.put({
                        'topic': topic_name.strip(),
                        'message': message
                    })
                    count += 1
            
            self.messages_queued += count
            print(f"✓ Queued {count} messages from file: {filepath}")
        
        except Exception as e:
            print(f"XXXXXXX Error reading file: {e}")
    
    def process_batch_input(self, batch_data):
        """Process batch input"""
        try:
            parts = batch_data.split('|', 1)
            if len(parts) != 2:
                print("⚠ Invalid batch format. Use: batch:topic_name|msg1;msg2;msg3")
                return
            
            topic_name, messages = parts
            topic_name = topic_name.strip()
            message_list = messages.split(';')
            
            count = 0
            for msg in message_list:
                msg = msg.strip()
                if msg:
                    message = {
                        'content': msg,
                        'timestamp': time.time(),
                        'producer_id': 'batch_producer',
                        'auto_generated': False
                    }
                    
                    self.message_queue.put({
                        'topic': topic_name,
                        'message': message
                    })
                    count += 1
            
            self.messages_queued += count
            print(f"✓ Queued {count} messages for topic: {topic_name}")
        
        except Exception as e:
            print(f"XXXXXXX Error processing batch: {e}")

    def topic_watcher_thread(self):
        """Creation / Deletion of topics dynamically"""
        print("✓ Topic Watcher Thread Started")
        
        while self.running:
            try:
                self.check_and_create_topics()
                self.check_and_delete_topics()
                time.sleep(3)
                
            except Exception as e:
                print(f"XXXXXXX Topic watcher error: {e}")
                time.sleep(5)
    
    def check_and_create_topics(self):
        """Check for approved topics and create them in Kafka"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute("SELECT topic_name FROM topics WHERE status = 'approved'")
            approved_topics = cursor.fetchall()
            
            for topic_row in approved_topics:
                topic_name = topic_row['topic_name']
                
                if topic_name not in self.created_topics:
                    if self.create_kafka_topic(topic_name):
                        cursor.execute(
                            "UPDATE topics SET status = 'active' WHERE topic_name = %s",
                            (topic_name,)
                        )
                        conn.commit()
                        
                        self.created_topics.add(topic_name)
                        self.known_topics.add(topic_name)
                        print(f"+++++++ Topic activated: {topic_name}")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"XXXXXXX Error checking approved topics: {e}")
    
    def check_and_delete_topics(self):
        """Check for rejected/deleted topics and remove them from Kafka"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute(
                "SELECT topic_name FROM topics WHERE status IN ('rejected', 'deleted')"
            )
            deleted_topics = cursor.fetchall()
            
            for topic_row in deleted_topics:
                topic_name = topic_row['topic_name']
                
                if topic_name in self.created_topics:
                    if self.delete_kafka_topic(topic_name):
                        self.created_topics.discard(topic_name)
                        self.known_topics.discard(topic_name)
                        print(f"🗑️  Topic deleted: {topic_name}")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"XXXXXXX Error checking deleted topics: {e}")
    
    def create_kafka_topic(self, topic_name, num_partitions=3, replication_factor=1):
        """Create a topic in Kafka using Admin API"""
        try:
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            self.admin_client.create_topics([topic], validate_only=False)
            print(f"✓ Kafka topic created: {topic_name}")
            return True
        except TopicAlreadyExistsError:
            print(f"⚠ Topic already exists in Kafka: {topic_name}")
            return True
        except Exception as e:
            print(f"XXXXXXX Failed to create Kafka topic {topic_name}: {e}")
            return False
    
    def delete_kafka_topic(self, topic_name):
        """Delete a topic from Kafka"""
        try:
            self.admin_client.delete_topics([topic_name])
            print(f"✓ Kafka topic deleted: {topic_name}")
            return True
        except UnknownTopicOrPartitionError:
            print(f"⚠ Topic doesn't exist in Kafka: {topic_name}")
            return True
        except Exception as e:
            print(f"XXXXXXX Failed to delete Kafka topic {topic_name}: {e}")
            return False

    def print_statistics(self):
        """Print producer statistics"""
        print("\n" + "="*60)
        print("PRODUCER STATISTICS")
        print("="*60)
        print(f"Auto-Streaming:     {'ENABLED' if self.auto_stream else 'DISABLED'}")
        print(f"Stream Interval:    {self.stream_interval} seconds")
        print(f"Messages Queued:    {self.messages_queued}")
        print(f"Messages Published: {self.messages_published}")
        print(f"Auto-Generated:     {self.messages_auto_generated}")
        print(f"Manual/File:        {self.messages_queued - self.messages_auto_generated}")
        print(f"Queue Size:         {self.message_queue.qsize()}")
        print(f"Active Topics:      {len(self.created_topics)}")
        if self.created_topics:
            print(f"Topics:             {', '.join(self.created_topics)}")
        print("="*60 + "\n")
    
    def print_active_topics(self):
        """Print currently active topics"""
        try:
            topics = self.get_active_topics_list()
            
            print("\n" + "="*60)
            print("ACTIVE TOPICS")
            print("="*60)
            if topics:
                for topic_name in topics:
                    indicator = "✓" if topic_name in self.created_topics else "⚠"
                    print(f"{indicator} {topic_name}")
            else:
                print("No active topics")
            print("="*60 + "\n")
        except Exception as e:
            print(f"XXXXXXX Error fetching topics: {e}")

    def start(self):
        """Start all threads"""
        print("\n" + "="*60)
        print("DYNAMIC KAFKA PRODUCER WITH AUTO-STREAMING")
        print("="*60)
        
        # START THREADS
        t1 = threading.Thread(target=self.publisher_thread, daemon=True, name="Publisher")
        t2 = threading.Thread(target=self.auto_streaming_thread, daemon=True, name="AutoStreaming")
        t3 = threading.Thread(target=self.input_listener_thread, daemon=True, name="InputListener")
        t4 = threading.Thread(target=self.topic_watcher_thread, daemon=True, name="TopicWatcher")
        
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        
        print(f"\n✓ All threads started successfully")
        print(f"✓ Auto-streaming: {'ENABLED' if self.auto_stream else 'DISABLED'}\n")
        
        # MAIN GUY ALIVE
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n⚠ Shutting down producer...")
            self.running = False
        
        # WAITING
        t1.join(timeout=3)
        t2.join(timeout=3)
        t3.join(timeout=3)
        t4.join(timeout=3)
        
        # STATS 
        self.print_statistics()
        
        # CLOSE ALL
        self.producer.close()
        self.admin_client.close()
        print("✓ Producer shutdown complete")

if __name__ == "__main__":
    # CHOICES PARCING
    auto_stream = True      
    if len(sys.argv) > 1:
        if sys.argv[1].lower() == '--no-auto':
            auto_stream = False
    
    producer = DynamicKafkaProducer(auto_stream=auto_stream)
    producer.start()
