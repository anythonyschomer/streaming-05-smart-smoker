"""
BBQ Consumer
Author: Anthony Schomer
Date: June 2, 2024
"""

import pika
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    temperature = float(body.decode())
    logging.info(f"Received smoker temperature: {temperature}")
    if temperature < 225 or temperature > 250:
        logging.warning(f"Smoker Alert! Temperature out of range: {temperature}")

def callback_food_a(ch, method, properties, body):
    """
    Callback function for the food item A queue.
    Args:
        ch: RabbitMQ channel
        method: Delivery information
        properties: Message properties
        body: Message body
    """
    temperature = float(body.decode())
    logging.info(f"Received food item A temperature: {temperature}")
    if temperature < 140:
        logging.warning(f"Food A Stall Alert! Temperature too low: {temperature}")

def callback_food_b(ch, method, properties, body):
    """
    Callback function for the food item B queue.
    Args:
        ch: RabbitMQ channel
        method: Delivery information
        properties: Message properties
        body: Message body
    """
    temperature = float(body.decode())
    logging.info(f"Received food item B temperature: {temperature}")
    if temperature < 140:
        logging.warning(f"Food B Stall Alert! Temperature too low: {temperature}")

def main():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        channel = connection.channel()

        # Ensure queues exist
        channel.queue_declare(queue=rabbitmq_queue_01, durable=True)
        channel.queue_declare(queue=rabbitmq_queue_02, durable=True)
        channel.queue_declare(queue=rabbitmq_queue_03, durable=True)

        # Set up consumers
        channel.basic_consume(queue=rabbitmq_queue_01, auto_ack=True, on_message_callback=callback_smoker)
        channel.basic_consume(queue=rabbitmq_queue_02, auto_ack=True, on_message_callback=callback_food_a)
        channel.basic_consume(queue=rabbitmq_queue_03, auto_ack=True, on_message_callback=callback_food_b)

        logging.info("Waiting for messages. To exit, press CTRL+C")
        channel.start_consuming()
    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"Error connecting to RabbitMQ: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()