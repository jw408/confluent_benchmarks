#!/usr/bin/env python
import time
import threading
import json
import uuid
import random
import os
import resource
from confluent_kafka import Producer, KafkaError, KafkaException
from collections import deque
from hdrh.histogram import HdrHistogram
from dotenv import load_dotenv
from functools import partial

# Load API Key and Secret from environment variables

load_dotenv()


BOOTSTRAP_SERVERS = os.environ.get("CONFLUENT_CLUSTER_URL")  # Your Confluent Cloud cluster
API_KEY = os.environ.get("CONFLUENT_API_KEY")
API_SECRET = os.environ.get("CONFLUENT_API_SECRET")

# edit these parameters to taste

TOPIC_NAME = "topic_0"
NUM_PRODUCERS = 64  # Number of producer threads.
MESSAGES_PER_PRODUCER = 50000  # Total messages per producer
TARGET_MESSAGES_PER_SECOND = 100000 # effectively unlimited for my cable modem connection
BATCH_SIZE = 16384  # Kafka batch size (bytes)
LINGER_MS = 10
ENABLE_IDEMPOTENCE = True
HISTOGRAM_MIN_VALUE = 1
HISTOGRAM_MAX_VALUE = 60000  # 60 seconds
HISTOGRAM_SIGNIFICANT_DIGITS = 3
MESSAGES_PER_BATCH = 500  

# --- Latency Histogram Configuration ---
HISTOGRAM_MIN_VALUE = 1  
HISTOGRAM_MAX_VALUE = 60000  
HISTOGRAM_SIGNIFICANT_DIGITS = 4

# --- File Descriptor Limit Functions ---
# without this, on Ubuntu, default soft limit is something low like 256 and so you can only spin up a paltry number of threads

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


def generate_message():
    return json.dumps({
        "user_id": str(uuid.uuid4()),
        "timestamp": time.time(),
        "ad_id": random.randint(1, 10000),
        "message": "This is a message!",
        "some_data": "x" * 512
    })

# --- Producer Class ---

class BulkMessageProducer:
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
        self.previous_produce_time_ns = None  # Track the *previous* produce time


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
            'acks': 'all',  # CRITICAL: Must be 'all'
            'retries': 2147483647,
            'max.in.flight.requests.per.connection': 5,
            'compression.type': 'lz4',
            'enable.idempotence': ENABLE_IDEMPOTENCE,
        }
        return Producer(conf)

    def _delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
            self.error_count += 1

    def produce_messages(self):
        messages_per_ms = TARGET_MESSAGES_PER_SECOND / NUM_PRODUCERS / 1000
        sleep_duration = (MESSAGES_PER_BATCH / messages_per_ms) / 1000.0
        sleep_duration = max(sleep_duration, 0.0001)

        remaining_messages = MESSAGES_PER_PRODUCER

        while remaining_messages > 0:
            batch_size = min(MESSAGES_PER_BATCH, remaining_messages)
            for _ in range(batch_size):
                message = generate_message()
                try:
                    # --- Measure time *before* produce() ---
                    current_produce_time_ns = time.time_ns()

                    if self.previous_produce_time_ns is not None:
                        # Calculate the *difference* in time
                        latency_ns = current_produce_time_ns - self.previous_produce_time_ns
                        self.latency_histogram.record_value(latency_ns)

                    self.previous_produce_time_ns = current_produce_time_ns  # Update for next iteration
                    # ---

                    self.producer.produce(
                        TOPIC_NAME,
                        message.encode('utf-8'),
                        callback=self._delivery_report # Use default delivery report
                    )
                    self.sent_count += 1

                except KafkaException as e:
                    print(f"Producer {self.producer_id} error: {e}")
                    self.error_count += 1
                    #  Crucially, do *not* update previous_produce_time_ns here
                    #  because the produce call failed.
                except BufferError:
                    print(f"Producer {self.producer_id} buffer full, waiting...")
                    time.sleep(0.1)
                    #  Do *not* update previous_produce_time_ns here either.

            remaining_messages -= batch_size
            time.sleep(sleep_duration) # Sleeps to prevent tight loop

            if time.time() - self.last_flush_time > 1.0:
                self.producer.flush()
                self.last_flush_time = time.time()

        self.producer.flush()  # Final flush
        self.stats_queue.append((self.sent_count, self.error_count, self.latency_histogram))
        print(f"Producer {self.producer_id} finished. Sent: {self.sent_count}, Errors: {self.error_count}")



def main():
    if not API_KEY or not API_SECRET or not BOOTSTRAP_SERVERS:
        print("Error: CONFLUENT_API_KEY and CONFLUENT_API_SECRET and BOOTSTRAP_SERVERS environment variables must be set; use the .env file")
        return

    print_file_descriptor_info()
    set_file_descriptor_limit(60000)
    print_file_descriptor_info()

    print("Starting Producer Benchmark...")
    start_time = time.time()

    stats_queue = deque()
    threads = []
    for i in range(NUM_PRODUCERS):
        producer = BulkMessageProducer(i, stats_queue)
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
    print(f"Throughput max rate limit: {TARGET_MESSAGES_PER_SECOND} messages/second")
    if duration > 0:
        print(f"Achieved Throughput: {total_sent / duration:.2f} messages/second")
    if total_sent > 0:
        print(f"Error Rate: {(total_errors / total_sent) * 100:.4f}%")

    print("\n--- Single Message Transmission Latency Statistics (milliseconds) ---")
    print(f"  p50:  {combined_latency_histogram.get_value_at_percentile(50.0) / 1_000_000.0:.2f} ms")
    print(f"  p90:  {combined_latency_histogram.get_value_at_percentile(90.0) / 1_000_000.0:.2f} ms")
    print(f"  p95:  {combined_latency_histogram.get_value_at_percentile(95.0) / 1_000_000.0:.2f} ms")
    print(f"  p99:  {combined_latency_histogram.get_value_at_percentile(99.0) / 1_000_000.0:.2f} ms")
    print(f"  p99.9: {combined_latency_histogram.get_value_at_percentile(99.9) / 1_000_000.0:.2f} ms")
    print(f"  Max:  {combined_latency_histogram.get_max_value() / 1_000_000.0:.2f} ms")
    print(f"  Min:  {combined_latency_histogram.get_min_value() / 1_000_000.0:.2f} ms")

if __name__ == "__main__":
    main()