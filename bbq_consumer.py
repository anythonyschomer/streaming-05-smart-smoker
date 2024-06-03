"""
BBQ Consumer
Author: Anthony Schomer
Date: June 2, 2024
"""

import pika

# RabbitMQ connection parameters
rabbitmq_host = "localhost"
rabbitmq_queue_01 = "01-smoker"
rabbitmq_queue_02 = "02-food-A"
rabbitmq_queue_03 = "03-food-B"

def callback_smoker(ch, method, properties, body):
    """
    Callback function for the smoker queue.
    Args:
        ch: RabbitMQ channel
        method: Delivery information
        properties: Message properties
        body: Message body
    """
    print(f"Received smoker temperature: {body.decode()}")

def callback_food_a(ch, method, properties, body):
    """
    Callback function for the food item A queue.
    Args:
        ch: RabbitMQ channel
        method: Delivery information
        properties: Message properties
        body: Message body
    """
    print(f"Received food item A temperature: {body.decode()}")

def callback_food_b(ch, method, properties, body):
    """
    Callback function for the food item B queue.
    Args:
        ch: RabbitMQ channel
        method: Delivery information
        properties: Message properties
        body: Message body
    """
    print(f"Received food item B temperature: {body.decode()}")

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
    channel = connection.channel()

    # Set up consumers
    channel.basic_consume(queue=rabbitmq_queue_01, auto_ack=True, on_message_callback=callback_smoker)
    channel.basic_consume(queue=rabbitmq_queue_02, auto_ack=True, on_message_callback=callback_food_a)
    channel.basic_consume(queue=rabbitmq_queue_03, auto_ack=True, on_message_callback=callback_food_b)

    print("Waiting for messages. To exit, press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    main()