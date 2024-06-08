"""
BBQ Producer
Author: Anthony Schomer
Date: June 7, 2024
"""

import csv
import pika
import time
import sys
import subprocess
import os  # Import the os module

# RabbitMQ connection parameters
rabbitmq_host = "localhost"
rabbitmq_queue_01 = "01-smoker"
rabbitmq_queue_02 = "02-food-A"
rabbitmq_queue_03 = "03-food-B"

## Creating a Connection Object
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))

## Communication Channel
channel = connection.channel()

## Declaring Queues
channel.queue_declare(queue=rabbitmq_queue_01)
channel.queue_declare(queue=rabbitmq_queue_02)
channel.queue_declare(queue=rabbitmq_queue_03)

## Declaring Callback Functions
def smoker_callback(ch, method, properties, body):
    print(f"Smoker received: {body}")

def foodA_callback(ch, method, properties, body):
    print(f"FoodA received: {body}")

def foodB_callback(ch, method, properties, body):
    print(f"FoodB received: {body}")

## Consuming Messages
channel.basic_consume(queue=rabbitmq_queue_01, on_message_callback=smoker_callback, auto_ack=True)
channel.basic_consume(queue=rabbitmq_queue_02, on_message_callback=foodA_callback, auto_ack=True)
channel.basic_consume(queue=rabbitmq_queue_03, on_message_callback=foodB_callback, auto_ack=True)

print('Waiting for messages. To exit, press CTRL+C')
channel.start_consuming()

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

def view_queue_messages(queue_name, requeue=False):
    """
    View messages in a RabbitMQ queue using the rabbitmqadmin tool.

    Args:
        queue_name (str): The name of the queue to view messages from.
        requeue (bool, optional): Whether to requeue the messages after viewing them. Defaults to False.
    """
    requeue_option = "requeue=true" if requeue else "requeue=false"
    command = f"rabbitmqadmin get queue={queue_name} {requeue_option}"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    print(result.stdout)

def main():
    smoker_temps_file = "smoker-temps.csv"
    food_a_temps_file = "food-a-temps.csv"
    food_b_temps_file = "food-b-temps.csv"

    # Get the current working directory
    cwd = os.getcwd()
    print("Current working directory:", cwd)

    # Create the food-a-temps.csv file if it doesn't exist
    file_path = os.path.join(cwd, "food-a-temps.csv")
    if not os.path.exists(file_path):
        with open(file_path, "w"):
            pass
        print(f"Created empty file: {file_path}")
    else:
        print(f"File already exists: {file_path}")

    print("Starting to process smoker-temps.csv...")
    sys.stdout.flush()  # Flush the output buffer

    connection, channel = get_rabbitmq_connection()

    send_messages(channel, rabbitmq_queue_01, smoker_temps_file)
    send_messages(channel, rabbitmq_queue_02, food_a_temps_file)
    send_messages(channel, rabbitmq_queue_03, food_b_temps_file)

    # View messages in the 02-food-A queue
    view_queue_messages(rabbitmq_queue_02, requeue=True)

    connection.close()

if __name__ == "__main__":
    main()