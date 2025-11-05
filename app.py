from flask import Flask, jsonify, render_template
import pika, ssl, sqlite3
from datetime import datetime

app = Flask(__name__)

# Queue configuration
QUEUES = [
    {"name": "Finance_invoices", "vhost": "kwvizzuf", "max": 1000, "threshold": 80},
    {"name": "Finance_payments", "vhost": "kwvizzuf", "max": 500, "threshold": 70},
    {"name": "Orders_failed_orders", "vhost": "kwvizzuf", "max": 2000, "threshold": 85},
    {"name": "Orders_new_orders", "vhost": "kwvizzuf", "max": 300, "threshold": 60},
]

CLOUDAMQP_URL = "amqps://kwvizzuf:fyF1bexQjw7vJi7vTwl8ONm_Gg3XRGFj@armadillo.rmq.cloudamqp.com/kwvizzuf"
DB_FILE = "queue_history.db"

# Initialize DB
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS queue_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            queue_name TEXT,
            message_count INTEGER,
            max_capacity INTEGER,
            threshold INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

init_db()

# Fetch queue count
def get_queue_count(queue_name):
    params = pika.URLParameters(CLOUDAMQP_URL)
    params.ssl_options = pika.SSLOptions(context=ssl.create_default_context())
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    q = channel.queue_declare(queue=queue_name, passive=True)
    count = q.method.message_count
    connection.close()
    return count

# Save historical data
def save_history(data):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    for q in data:
        cursor.execute('''
            INSERT INTO queue_history(queue_name, message_count, max_capacity, threshold)
            VALUES (?, ?, ?, ?)
        ''', (q['name'], q['count'], q['max'], q['threshold']))
    conn.commit()
    conn.close()

# Optional alerting (console)
def check_alerts(data):
    for q in data:
        if q['count'] >= (q['threshold']/100) * q['max']:
            print(f"ALERT: Queue {q['name']} crossed threshold! ({q['count']}/{q['max']})")

# API endpoint to fetch current counts
@app.route("/queue_counts")
def queue_counts():
    data = []
    for q in QUEUES:
        try:
            count = get_queue_count(q['name'])
        except Exception as e:
            count = None
        data.append({
            "name": q['name'],
            "count": count,
            "max": q['max'],
            "threshold": q['threshold']
        })
    save_history(data)
    check_alerts(data)
    return jsonify(data)

# API endpoint for historical data
@app.route("/queue_history/<queue_name>")
def queue_history(queue_name):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT timestamp, message_count FROM queue_history
        WHERE queue_name=? ORDER BY timestamp ASC
    ''', (queue_name,))
    rows = cursor.fetchall()
    conn.close()
    result = [{"timestamp": r[0], "count": r[1]} for r in rows]
    return jsonify(result)

# Dashboard
@app.route("/")
def index():
    return render_template("dashboard.html", queues=QUEUES)

if __name__ == "__main__":
    app.run(debug=True)
