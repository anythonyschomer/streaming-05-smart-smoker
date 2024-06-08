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

def send_messages(channel, queue_name, csv_file):
    """
    Send messages from a CSV file to a RabbitMQ queue.

    Args:
        channel (pika.channel.Channel): The RabbitMQ channel object.
        queue_name (str): The name of the queue to send messages to.
        csv_file (str): The path to the CSV file containing the messages.
    """
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            message = ' '.join(row)
            channel.basic_publish(exchange='',
                                  routing_key=queue_name,
                                  body=message.encode())
            print(f"Sent message: {message}")

def main():
    input_file_name = "smoker-temps.csv"  # Replace with the actual file name

    print("Starting to process smoker-temps.csv...")
    sys.stdout.flush()  # Flush the output buffer

    connection, channel = get_rabbitmq_connection()

    send_messages(channel, rabbitmq_queue_01, input_file_name)
    send_messages(channel, rabbitmq_queue_02, input_file_name)
    send_messages(channel, rabbitmq_queue_03, input_file_name)

    connection.close()

if __name__ == "__main__":
    main()