from flask import Flask, render_template, jsonify
import mysql.connector

app = Flask(__name__)

DB_CONFIG = {
    'host': 'localhost',
    'user': 'kafka_user',
    'password': 'Kafka@Pass123',
    'database': 'kafka_streaming'
}

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/topics')
def get_topics():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM topics ORDER BY created_at DESC")
    topics = cursor.fetchall()
    cursor.close()
    conn.close()
    
    # Convert datetime to string
    for topic in topics:
        topic['created_at'] = str(topic['created_at'])
        topic['updated_at'] = str(topic['updated_at'])
    
    return jsonify(topics)

@app.route('/api/subscriptions')
def get_subscriptions():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT us.user_id, us.topic_name, t.status, us.subscribed_at
        FROM user_subscriptions us
        JOIN topics t ON us.topic_name = t.topic_name
        ORDER BY us.subscribed_at DESC
    """)
    subscriptions = cursor.fetchall()
    cursor.close()
    conn.close()
    
    # Convert datetime to string
    for sub in subscriptions:
        sub['subscribed_at'] = str(sub['subscribed_at'])
    
    return jsonify(subscriptions)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
