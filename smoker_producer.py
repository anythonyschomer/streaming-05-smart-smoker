#!/usr/bin/env python
import pika
import csv
import time

# RabbitMQ server credentials
credentials = pika.PlainCredentials('anythonyschomer', 'Erin2024!')
parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)

# Connect to RabbitMQ server
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Declare the queues
channel.queue_declare(queue='01-smoker')
channel.queue_declare(queue='02-food-A')
channel.queue_declare(queue='03-food-B')

# Function to read data from CSV and publish to queues
def publish_temperatures(csv_file):
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for row in reader:
            timestamp, smoker_temp, food_A_temp, food_B_temp = row
            
            # Publish to respective queues
            channel.basic_publish(exchange='', routing_key='01-smoker', body=smoker_temp)
            channel.basic_publish(exchange='', routing_key='02-food-A', body=food_A_temp)
            channel.basic_publish(exchange='', routing_key='03-food-B', body=food_B_temp)
            
            print(f"Published temperatures at {timestamp}")
            time.sleep(30)  # Wait for 30 seconds (half a minute)

# Call the publish_temperatures function with the CSV file path
publish_temperatures('smoker-temps.csv')

# Close the connection
connection.close()