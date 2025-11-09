"""
Kafka Broker Monitor - For Broker Administrator Only
No producing or consuming - only monitoring and management
"""

from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
from kafka.errors import KafkaError, NoBrokersAvailable
import time
import sys
import socket
from datetime import datetime

def test_kafka_port(host, port):
    """Test if Kafka port is accessible"""
    print(f"Testing connection to {host}:{port}...")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print(f"✓ Port {port} is OPEN on {host}")
            return True
        else:
            print(f"❌ Port {port} is CLOSED on {host}")
            return False
    except socket.error as e:
        print(f"❌ Socket error: {e}")
        return False

class KafkaBrokerMonitor:
    def __init__(self, bootstrap_servers='172.27.111.37:9092', max_retries=5):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = None
        self.max_retries = max_retries
        
        # Test port first
        print("\n" + "="*70)
        print("PORT CONNECTIVITY TEST")
        print("="*70)
        host = bootstrap_servers.split(':')[0]
        port = int(bootstrap_servers.split(':')[1])
        
        print("\nTesting localhost...")
        test_kafka_port('localhost', port)
        
        print(f"\nTesting actual IP ({host})...")
        if not test_kafka_port(host, port):
            print("\n⚠ WARNING: Port is not accessible!")
            print("Check firewall: sudo ufw allow 9092")
        
        print("\n" + "="*70)
        print(f"🔧 Connecting to Kafka Broker: {bootstrap_servers}")
        print("="*70)
        
        # Try multiple connection attempts
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Attempt {attempt}/{max_retries}...")
                
                self.admin_client = KafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers,
                    client_id='broker_monitor',
                    request_timeout_ms=10000,
                    api_version_auto_timeout_ms=5000
                )
                
                # Test connection
                topics = self.admin_client.list_topics()
                
                print(f"✓ Connected to Kafka Broker")
                print(f"✓ Found {len(topics)} topics")
                print("="*70)
                return
                
            except NoBrokersAvailable:
                print(f"❌ No brokers available")
                if attempt < max_retries:
                    print(f"Retrying in 3 seconds...")
                    time.sleep(3)
                else:
                    print("\n" + "="*70)
                    print("TROUBLESHOOTING")
                    print("="*70)
                    print("1. Check Kafka is running: ps aux | grep kafka")
                    print("2. Check port: sudo netstat -tulpn | grep 9092")
                    print("3. Check server.properties listeners/advertised.listeners")
                    print("4. Restart Kafka if needed")
                    print("="*70)
                    sys.exit(1)
                    
            except Exception as e:
                print(f"❌ Error: {e}")
                if attempt >= max_retries:
                    sys.exit(1)
                time.sleep(3)
    
    def list_topics(self):
        """List all topics in Kafka"""
        try:
            topics = self.admin_client.list_topics()
            
            print("\n" + "="*70)
            print("KAFKA TOPICS")
            print("="*70)
            
            user_topics = [t for t in topics if not t.startswith('__')]
            internal_topics = [t for t in topics if t.startswith('__')]
            
            print(f"\nUser Topics ({len(user_topics)}):")
            if user_topics:
                for i, topic in enumerate(user_topics, 1):
                    print(f"  {i}. {topic}")
            else:
                print("  (None)")
            
            if internal_topics:
                print(f"\nInternal Topics ({len(internal_topics)}):")
                for topic in internal_topics:
                    print(f"  • {topic}")
            
            print("="*70 + "\n")
            return user_topics
        
        except Exception as e:
            print(f"❌ Error: {e}")
            return []
    
    def describe_topic(self, topic_name):
        """Show topic details"""
        try:
            metadata = self.admin_client.describe_topics([topic_name])
            
            print(f"\n{'='*70}")
            print(f"TOPIC: {topic_name}")
            print("="*70)
            
            for topic_metadata in metadata:
                print(f"Partitions: {len(topic_metadata['partitions'])}")
                
                for partition in topic_metadata['partitions']:
                    print(f"\n  Partition {partition['partition']}:")
                    print(f"    Leader: {partition['leader']}")
                    print(f"    Replicas: {partition['replicas']}")
                    print(f"    ISR: {partition['isr']}")
            
            print("="*70 + "\n")
        
        except Exception as e:
            print(f"❌ Error: {e}")
    
    def check_topic_health(self, topic_name):
        """Check if topic is healthy"""
        try:
            metadata = self.admin_client.describe_topics([topic_name])
            
            print(f"\n{'='*70}")
            print(f"HEALTH CHECK: {topic_name}")
            print("="*70)
            
            all_healthy = True
            
            for topic_metadata in metadata:
                for partition in topic_metadata['partitions']:
                    pid = partition['partition']
                    leader = partition['leader']
                    replicas = partition['replicas']
                    isr = partition['isr']
                    
                    if leader == -1:
                        print(f"❌ Partition {pid}: NO LEADER")
                        all_healthy = False
                    else:
                        print(f"✓ Partition {pid}: Leader {leader}")
                    
                    if set(replicas) == set(isr):
                        print(f"  ✓ Replicas in-sync: {isr}")
                    else:
                        print(f"  ⚠ Out-of-sync replicas!")
                        print(f"    Replicas: {replicas}, ISR: {isr}")
                        all_healthy = False
            
            if all_healthy:
                print("\n✓ Topic is healthy")
            else:
                print("\n⚠ Topic has issues")
            
            print("="*70 + "\n")
        
        except Exception as e:
            print(f"❌ Error: {e}")
    
    def get_broker_info(self):
        """Show broker information"""
        try:
            print(f"\n{'='*70}")
            print("BROKER INFORMATION")
            print("="*70)
            
            cluster = self.admin_client._client.cluster
            brokers = cluster.brokers()
            
            print(f"\nActive Brokers: {len(brokers)}")
            for broker in brokers:
                print(f"  • Broker {broker.nodeId}: {broker.host}:{broker.port}")
            
            print("="*70 + "\n")
        
        except Exception as e:
            print(f"❌ Error: {e}")
    
    def check_connectivity(self):
        """Test broker connectivity"""
        try:
            print(f"\n{'='*70}")
            print("CONNECTIVITY CHECK")
            print("="*70)
            
            topics = self.admin_client.list_topics()
            print(f"✓ Connected to broker")
            print(f"✓ Can access {len(topics)} topics")
            
            cluster = self.admin_client._client.cluster
            brokers = cluster.brokers()
            print(f"✓ Found {len(brokers)} broker(s)")
            
            print("\n✓ All checks passed")
            print("="*70 + "\n")
            return True
        
        except Exception as e:
            print(f"❌ Failed: {e}")
            print("="*70 + "\n")
            return False
    
    def watch_topics(self, interval=5):
        """Watch for topic changes in real-time"""
        print(f"\n{'='*70}")
        print("WATCHING FOR TOPIC CHANGES")
        print(f"Checking every {interval} seconds (Ctrl+C to stop)")
        print("="*70 + "\n")
        
        known_topics = set()
        
        try:
            while True:
                current_topics = set(self.admin_client.list_topics())
                current_topics = {t for t in current_topics if not t.startswith('__')}
                
                # Check for new topics
                new_topics = current_topics - known_topics
                if new_topics:
                    for topic in new_topics:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] ➕ NEW: {topic}")
                
                # Check for deleted topics
                deleted_topics = known_topics - current_topics
                if deleted_topics:
                    for topic in deleted_topics:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] ➖ DELETED: {topic}")
                
                known_topics = current_topics
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n\n✓ Stopped watching")
    
    def run_monitor(self):
        """Main monitoring interface"""
        print("\n" + "="*70)
        print("KAFKA BROKER MONITOR")
        print("="*70)
        
        while True:
            print("\n1. List All Topics")
            print("2. Describe Topic")
            print("3. Check Topic Health")
            print("4. Show Broker Info")
            print("5. Test Connectivity")
            print("6. Watch Topics (Real-time)")
            print("7. Full System Check")
            print("8. Exit")
            
            choice = input("\nChoice: ").strip()
            
            if choice == '1':
                self.list_topics()
            
            elif choice == '2':
                topics = self.list_topics()
                if topics:
                    name = input("Topic name: ").strip()
                    if name:
                        self.describe_topic(name)
            
            elif choice == '3':
                topics = self.list_topics()
                if topics:
                    name = input("Topic name: ").strip()
                    if name:
                        self.check_topic_health(name)
            
            elif choice == '4':
                self.get_broker_info()
            
            elif choice == '5':
                self.check_connectivity()
            
            elif choice == '6':
                interval = input("Check interval (seconds, default 5): ").strip()
                interval = int(interval) if interval else 5
                self.watch_topics(interval)
            
            elif choice == '7':
                print("\n" + "="*70)
                print("FULL SYSTEM CHECK")
                print("="*70)
                
                self.check_connectivity()
                topics = self.list_topics()
                self.get_broker_info()
                
                if topics:
                    print("\nChecking topic health...\n")
                    for topic in topics:
                        self.check_topic_health(topic)
                        time.sleep(1)
                
                print("✓ Full check complete\n")
            
            elif choice == '8':
                print("\nExiting...")
                if self.admin_client:
                    self.admin_client.close()
                break
            
            else:
                print("Invalid choice")

if __name__ == "__main__":
    broker = sys.argv[1] if len(sys.argv) > 1 else '172.27.111.37:9092'
    
    try:
        monitor = KafkaBrokerMonitor(broker)
        monitor.run_monitor()
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted")
    finally:
        print("✓ Shutdown complete")

