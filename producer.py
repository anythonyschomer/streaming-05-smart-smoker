# producer.py
import pika
import time
import csv

# Establish a connection to the RabbitMQ server
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Declare the three queues
channel.queue_declare(queue='01-smoker', durable=True)
channel.queue_declare(queue='02-food-A', durable=True)
channel.queue_declare(queue='03-food-B', durable=True)

# Open the smoker-temps.csv file
with open('smoker-temps.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row

    # Read each line from the file
    for row in reader:
        time_stamp, channel1, channel2, channel3 = row

        # Send the appropriate value to the corresponding queue
        channel.basic_publish(exchange='',
                              routing_key='01-smoker',
                              body=channel1)
        channel.basic_publish(exchange='',
                              routing_key='02-food-A',
                              body=channel2)
        channel.basic_publish(exchange='',
                              routing_key='03-food-B',
                              body=channel3)

        print(f"Sent data: {time_stamp}, {channel1}, {channel2}, {channel3}")
        time.sleep(30)  # Sleep for 30 seconds before reading the next line

# Close the connection
connection.close()