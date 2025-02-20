#!/usr/bin/env python
import time
import threading
import json
import uuid
import random
import os  # Import the 'os' module
import resource
from confluent_kafka import Producer, KafkaError, KafkaException
from collections import deque
from hdrh.histogram import HdrHistogram
from dotenv import load_dotenv
from functools import partial  # Import functools.partial

# confluent kafka topic update topic_0 --config "message.timestamp.type=LogAppendTime" --cluster <your_cluster_id>

load_dotenv()

# --- Configuration (REPLACE WITH YOUR VALUES, except API key/secret) ---
BOOTSTRAP_SERVERS = os.environ.get("CONFLUENT_CLUSTER_URL")  # Your Confluent Cloud cluster
# Load API Key and Secret from environment variables
API_KEY = os.environ.get("CONFLUENT_API_KEY")
API_SECRET = os.environ.get("CONFLUENT_API_SECRET")
TOPIC_NAME = "topic_0"
NUM_PRODUCERS = 32  # Number of producer threads.
MESSAGES_PER_PRODUCER = 10000  # Total messages per producer
TARGET_MESSAGES_PER_SECOND = 20000000  # Still targeting 20M/s, even for the small run
BATCH_SIZE = 16384  # Kafka batch size (bytes)
LINGER_MS = 10
ENABLE_IDEMPOTENCE = True
HISTOGRAM_MIN_VALUE = 1
HISTOGRAM_MAX_VALUE = 60000  # 60 seconds
HISTOGRAM_SIGNIFICANT_DIGITS = 3
MESSAGES_PER_BATCH = 250  

# --- Latency Histogram Configuration ---
HISTOGRAM_MIN_VALUE = 1  # 1 millisecond (adjust if needed)
HISTOGRAM_MAX_VALUE = 60000  # 60 seconds (adjust if needed)
HISTOGRAM_SIGNIFICANT_DIGITS = 3

# --- File Descriptor Limit Functions ---

def get_file_descriptor_limits():
    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    return soft_limit, hard_limit

def print_file_descriptor_info():
    soft_limit, hard_limit = get_file_descriptor_limits()
    print(f"Soft File Descriptor Limit: {soft_limit}")
    print(f"Hard File Descriptor Limit: {hard_limit}")
    try:
        print(f"Current Open FDs (approximate): {len(os.listdir('/proc/self/fd'))}")
    except FileNotFoundError:
        print("Cannot determine current open FDs (not on Linux/macOS).")

def set_file_descriptor_limit(new_soft_limit):
    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    if new_soft_limit > hard_limit:
        print(f"Warning: Requested soft limit ({new_soft_limit}) exceeds hard limit ({hard_limit}). Setting to hard limit.")
        new_soft_limit = hard_limit
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft_limit, hard_limit))
        print(f"Soft file descriptor limit set to: {new_soft_limit}")
    except ValueError as e:
        print(f"Error setting file descriptor limit: {e}")
    except OSError as e:
        print(f"Error setting file descriptor limit (OSError): {e}")


# --- Helper Functions ---

def generate_message():
    return json.dumps({
        "user_id": str(uuid.uuid4()),
        "timestamp": time.time(),  # Still useful to have in the message
        "ad_id": random.randint(1, 100),
        "message": "This is a Super Bowl ad message!",
        "some_data": "x" * 512
    })

# --- Producer Class ---

class SuperBowlProducer:
    def __init__(self, producer_id, stats_queue):
        self.producer_id = producer_id
        self.stats_queue = stats_queue
        self.producer = self._create_producer()
        self.sent_count = 0
        self.error_count = 0
        self.last_flush_time = time.time()
        self.latency_histogram = HdrHistogram(
            HISTOGRAM_MIN_VALUE, HISTOGRAM_MAX_VALUE, HISTOGRAM_SIGNIFICANT_DIGITS
        )
        self.start_time_ns = None  # Initialize start time
        self.first_broker_timestamp_ns = None  # For relative broker time

    def _create_producer(self):
        conf = {
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': API_KEY,
            'sasl.password': API_SECRET,
            'client.id': f'producer-{self.producer_id}',
            'batch.size': BATCH_SIZE,
            'linger.ms': LINGER_MS,
            'acks': 'all',
            'retries': 2147483647,
            'max.in.flight.requests.per.connection': 5,
            'compression.type': 'lz4',
            'enable.idempotence': ENABLE_IDEMPOTENCE,
        }
        return Producer(conf)

    def _delivery_report_with_time(self, err, msg, send_time):
        if err is not None:
            print(f"Message delivery failed: {err}")
            self.error_count += 1
        else:
            if msg.timestamp()[0] == -1:
                print(f"Warning: Topic '{msg.topic()}' is using CreateTime. Change to LogAppendTime.")
                return

            msg_timestamp_sec, msg_timestamp_usec = msg.timestamp()
            msg_timestamp_ns = (msg_timestamp_sec * 1_000_000_000) + (msg_timestamp_usec * 1_000)

            # --- CORRECTED LATENCY CALCULATION ---
            latency_ns = msg_timestamp_ns - send_time  # Broker time - Send time
            latency_ms = int(latency_ns / 1_000_000)

            try:
                self.latency_histogram.record_value(latency_ms)
            except Exception as e:
                print(f"Error recording latency: {e}, latency_ms: {latency_ms}")


    def produce_messages(self):
        # Record the start time *once* per producer.
        self.start_time_ns = time.time_ns()

        messages_per_ms = TARGET_MESSAGES_PER_SECOND / NUM_PRODUCERS / 1000
        sleep_duration = (MESSAGES_PER_BATCH / messages_per_ms) / 1000.0
        sleep_duration = max(sleep_duration, 0.0001)

        remaining_messages = MESSAGES_PER_PRODUCER

        while remaining_messages > 0:
            batch_size = min(MESSAGES_PER_BATCH, remaining_messages)
            for _ in range(batch_size):
                message = generate_message()
                try:
                    send_time = time.time_ns()
                    delivery_callback = partial(self._delivery_report_with_time, send_time=send_time)
                    self.producer.produce(
                        TOPIC_NAME,
                        message.encode('utf-8'),
                        callback=delivery_callback
                    )
                    self.sent_count += 1
                except KafkaException as e:
                    print(f"Producer {self.producer_id} error: {e}")
                    self.error_count += 1
                except BufferError:
                    print(f"Producer {self.producer_id} buffer full, waiting...")
                    time.sleep(0.1)

            remaining_messages -= batch_size
            time.sleep(sleep_duration)

            if time.time() - self.last_flush_time > 1.0:
                self.producer.flush()
                self.last_flush_time = time.time()

        self.producer.flush()
        self.stats_queue.append((self.sent_count, self.error_count, self.latency_histogram))
        print(f"Producer {self.producer_id} finished. Sent: {self.sent_count}, Errors: {self.error_count}")

# --- Main Execution ---

def main():
    if not API_KEY or not API_SECRET:
        print("Error: CONFLUENT_API_KEY and CONFLUENT_API_SECRET environment variables must be set.")
        return

    print_file_descriptor_info()
    set_file_descriptor_limit(60000)
    print_file_descriptor_info()

    print("Starting Super Bowl Ad Benchmark...")
    start_time = time.time()

    stats_queue = deque()
    threads = []
    for i in range(NUM_PRODUCERS):
        producer = SuperBowlProducer(i, stats_queue)
        thread = threading.Thread(target=producer.produce_messages)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    end_time = time.time()
    duration = end_time - start_time

    total_sent = 0
    total_errors = 0
    combined_latency_histogram = HdrHistogram(
        HISTOGRAM_MIN_VALUE, HISTOGRAM_MAX_VALUE, HISTOGRAM_SIGNIFICANT_DIGITS
    )

    while stats_queue:
        sent, errors, latency_histogram = stats_queue.popleft()
        total_sent += sent
        total_errors += errors
        combined_latency_histogram.add(latency_histogram)

    print("\n--- Benchmark Results ---")
    print(f"Total Time: {duration:.2f} seconds")
    print(f"Total Messages Sent: {total_sent}")
    print(f"Total Errors: {total_errors}")
    print(f"Target Throughput: {TARGET_MESSAGES_PER_SECOND} messages/second")
    if duration > 0:
        print(f"Achieved Throughput: {total_sent / duration:.2f} messages/second")
    if total_sent > 0:
        print(f"Error Rate: {(total_errors / total_sent) * 100:.4f}%")

    print("\n--- Latency Statistics (milliseconds) ---")
    print(f"  p50:  {combined_latency_histogram.get_value_at_percentile(50.0):.2f} ms")
    print(f"  p90:  {combined_latency_histogram.get_value_at_percentile(90.0):.2f} ms")
    print(f"  p95:  {combined_latency_histogram.get_value_at_percentile(95.0):.2f} ms")
    print(f"  p99:  {combined_latency_histogram.get_value_at_percentile(99.0):.2f} ms")
    print(f"  p99.9: {combined_latency_histogram.get_value_at_percentile(99.9):.2f} ms")
    print(f"  Max:  {combined_latency_histogram.get_max_value():.2f} ms")
    print(f"  Min:  {combined_latency_histogram.get_min_value():.2f} ms")
if __name__ == "__main__":
    main()