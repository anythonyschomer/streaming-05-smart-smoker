"""
BBQ Producer
Author: Anthony Schomer
Date: June 2, 2024
"""

import csv
import pika
import time
import sys  # Import sys module

# RabbitMQ connection parameters
rabbitmq_host = "localhost"
rabbitmq_queue_01 = "01-smoker"
rabbitmq_queue_02 = "02-food-A"
rabbitmq_queue_03 = "03-food-B"

def get_rabbitmq_connection():
    """
    Create a RabbitMQ connection and channel.
    Returns:
        connection, channel
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
    channel = connection.channel()
    return connection, channel

def publish_message(channel, queue_name, message):
    """
    Publish a message to a RabbitMQ queue.
    Args:
        channel: RabbitMQ channel
        queue_name: Name of the queue
        message: Message to be published
    """
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    print(f"Sent message to {queue_name}: {message}")
    sys.stdout.flush()  # Flush the output buffer

def main():
    input_file_name = "smoker-temps.csv"  # Replace with the actual file name

    print("Starting to process smoker-temps.csv...")
    sys.stdout.flush()  # Flush the output buffer

    with open(input_file_name, 'r', newline='') as input_file:
        reader = csv.reader(input_file)
        next(reader)  # Skip the header row

        # Reading rows from CSV
        for row in reader:
            timestamp = row[0]
            smoker_temp = row[1]
            food_a_temp = row[2]
            food_b_temp = row[3]

            # Publish messages to respective queues
            connection, channel = get_rabbitmq_connection()
            publish_message(channel, rabbitmq_queue_01, smoker_temp)
            publish_message(channel, rabbitmq_queue_02, food_a_temp)
            publish_message(channel, rabbitmq_queue_03, food_b_temp)

            connection.close()

            time.sleep(30)  # Wait for 30 seconds before reading the next row

if __name__ == "__main__":
    main()