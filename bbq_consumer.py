"""
BBQ Consumer
Author: Anthony Schomer
Date: June 7, 2024
"""

import pika
import logging
import re
from collections import deque

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# RabbitMQ connection parameters
rabbitmq_host = "localhost"
rabbitmq_queue_01 = "01-smoker"
rabbitmq_queue_02 = "02-food-A"
rabbitmq_queue_03 = "03-food-B"

# Window size (number of temperatures to consider)
window_size = 5

# Initialize deques for each queue
smoker_temps = deque(maxlen=window_size)
food_a_temps = deque(maxlen=window_size)
food_b_temps = deque(maxlen=window_size)

## Creating a Connection Object
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))

## Communication Channel
channel = connection.channel()

## Declaring Queues
channel.queue_declare(queue=rabbitmq_queue_01, durable=True)
channel.queue_declare(queue=rabbitmq_queue_02, durable=True)
channel.queue_declare(queue=rabbitmq_queue_03, durable=False)

## Declaring Callback Functions
def callback_smoker(ch, method, properties, body):
    """
    Callback function for the smoker queue.
    """
    temperature = parse_temperature(body)
    if temperature is not None:
        logging.info(f"Received smoker temperature: {temperature}")
        smoker_temps.append(temperature)
        check_temperature_change(smoker_temps, 225, 250)
    else:
        logging.warning(f"Invalid message format: {body.decode()}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

def callback_food_a(ch, method, properties, body):
    """
    Callback function for the food item A queue.
    """
    temperature = parse_temperature(body)
    if temperature is not None:
        logging.info(f"Received food item A temperature: {temperature}")
        food_a_temps.append(temperature)
        check_temperature_change(food_a_temps, 140, 180)
    else:
        logging.warning(f"Invalid message format: {body.decode()}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

def callback_food_b(ch, method, properties, body):
    """
    Callback function for the food item B queue.
    """
    temperature = parse_temperature(body)
    if temperature is not None:
        logging.info(f"Received food item B temperature: {temperature}")
        food_b_temps.append(temperature)
        check_temperature_change(food_b_temps, 140, 180)
    else:
        logging.warning(f"Invalid message format: {body.decode()}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

## Consuming Messages
channel.basic_consume(queue=rabbitmq_queue_01, auto_ack=False, on_message_callback=callback_smoker)
channel.basic_consume(queue=rabbitmq_queue_02, auto_ack=False, on_message_callback=callback_food_a)
channel.basic_consume(queue=rabbitmq_queue_03, auto_ack=False, on_message_callback=callback_food_b)

logging.info("Waiting for messages. To exit, press CTRL+C")
channel.start_consuming()

def check_temperature_change(temp_deque, min_temp, max_temp):
    """
    Check if the temperature change in the deque is within the specified range.
    Args:
        temp_deque: Deque containing temperatures
        min_temp: Minimum acceptable temperature
        max_temp: Maximum acceptable temperature
    """
    if len(temp_deque) >= window_size:
        temp_change = temp_deque[-1] - temp_deque[0]
        if temp_change < min_temp or temp_change > max_temp:
            logging.warning(f"Temperature change out of range: {temp_change}")

def parse_temperature(message_body):
    """
    Parse the temperature value from the message body.
    """
    match = re.search(r'(\d+\.\d+)', message_body.decode())
    if match:
        return float(match.group(1))
    else:
        return None

def main():
    try:
        # Connection and channel creation moved to the top
        logging.info("Waiting for messages. To exit, press CTRL+C")
        channel.start_consuming()
    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"Error connecting to RabbitMQ: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    main()