import mysql.connector
import time
from datetime import datetime

class KafkaAdmin:
    def __init__(self, host='localhost', user='kafka_user', password='Kafka@Pass123', database='kafka_streaming'):
        self.db_config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database
        }
    
    def get_connection(self):
        return mysql.connector.connect(**self.db_config)
    
    def fetch_pending_topics(self):
        conn = self.get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM topics WHERE status = 'pending'")
        topics = cursor.fetchall()
        cursor.close()
        conn.close()
        return topics
    
    def approve_topic(self, topic_name):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE topics SET status = 'approved' WHERE topic_name = %s", (topic_name,))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"✓ Approved topic: {topic_name}")
    
    def reject_topic(self, topic_name):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE topics SET status = 'rejected' WHERE topic_name = %s", (topic_name,))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"✗ Rejected topic: {topic_name}")
    
    def create_topic_request(self, topic_name):
        """Helper method to create topic requests"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("INSERT INTO topics (topic_name, status) VALUES (%s, 'pending')", (topic_name,))
            conn.commit()
            print(f"✓ Topic request created: {topic_name}")
        except mysql.connector.IntegrityError:
            print(f"⚠ Topic already exists: {topic_name}")
        cursor.close()
        conn.close()
    
    def run_admin_panel(self):
        print("=" * 60)
        print("KAFKA ADMIN PANEL")
        print("=" * 60)
        
        while True:
            print("\n1. View Pending Topics")
            print("2. Approve Topic")
            print("3. Reject Topic")
            print("4. Create Topic Request (for testing)")
            print("5. View All Topics")
            print("6. Exit")
            
            choice = input("\nEnter your choice: ").strip()
            
            if choice == '1':
                topics = self.fetch_pending_topics()
                if topics:
                    print("\n--- Pending Topics ---")
                    for topic in topics:
                        print(f"ID: {topic['id']}, Name: {topic['topic_name']}, Created: {topic['created_at']}")
                else:
                    print("No pending topics.")
            
            elif choice == '2':
                topic_name = input("Enter topic name to approve: ").strip()
                self.approve_topic(topic_name)
            
            elif choice == '3':
                topic_name = input("Enter topic name to reject: ").strip()
                self.reject_topic(topic_name)
            
            elif choice == '4':
                topic_name = input("Enter topic name to create: ").strip()
                self.create_topic_request(topic_name)
            
            elif choice == '5':
                conn = self.get_connection()
                cursor = conn.cursor(dictionary=True)
                cursor.execute("SELECT * FROM topics")
                topics = cursor.fetchall()
                if topics:
                    print("\n--- All Topics ---")
                    for topic in topics:
                        print(f"Name: {topic['topic_name']}, Status: {topic['status']}, Created: {topic['created_at']}")
                else:
                    print("No topics found.")
                cursor.close()
                conn.close()
            
            elif choice == '6':
                print("Exiting admin panel...")
                break
            
            else:
                print("Invalid choice. Please try again.")

if __name__ == "__main__":
    admin = KafkaAdmin()
    admin.run_admin_panel()
