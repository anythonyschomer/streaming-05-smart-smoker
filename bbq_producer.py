"""
    This program reads temperature data from a CSV file and sends the temperature
    data to RabbitMQ queues for further processing
    
    Author: Anthony Schomer
    Date: May 31, 2024
"""

# bbq_producer.py

import csv
import pika
import time

# RabbitMQ connection parameters
rabbitmq_host = "localhost"
rabbitmq_queue_01 = "01-smoker"
rabbitmq_queue_02 = "02-food-A"
rabbitmq_queue_03 = "03-food-B"

def get_rabbitmq_connection():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
    channel = connection.channel()
    return connection, channel

def publish_message(channel, queue_name, message):
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    print(f"Published message to {queue_name}: {message}")

def main():
    with open("smoker-temps.csv", "r") as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header row

        for row in reader:
            timestamp, smoker_temp, food_a_temp, food_b_temp = row

            # Publish messages to respective queues
            connection, channel = get_rabbitmq_connection()
            publish_message(channel, rabbitmq_queue_01, smoker_temp)
            publish_message(channel, rabbitmq_queue_02, food_a_temp)
            publish_message(channel, rabbitmq_queue_03, food_b_temp)

            connection.close()

            time.sleep(30)  # Wait for 30 seconds before reading the next row

if __name__ == "__main__":
    main()