"""
    This program reads temperature data from a CSV file and sends the temperature
    data to RabbitMQ queues for further processing
    
    Author: Anthony Schomer
    Date: May 31, 2024
"""

import pika
import sys
import webbrowser
import csv
from datetime import datetime
import time
import logging

logger = logging.getLogger(__name__)

from util_logger import setup_logger
logger, logname = setup_logger(__file__)


def offer_rabbitmq_admin_site(show_offer: bool = True):
    """Offer to open the RabbitMQ Admin website"""
    if not show_offer: 
        print("RabbitMQ Admin connection has been turned off.")
        return
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def rabbitmq_connection(host: str, queues: list):
    """
    Establishes a connection to the RabbitMQ server and declares queues.
    Parameters:
    host (str): the host name or IP address of the RabbitMQ server
    queues (list): list of queue names
    Returns:
    conn: the RabbitMQ connection object
    ch: the RabbitMQ channel object
    """
    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # delete and declare new durable queues
        for queue_name in queues:
            ch.queue_delete(queue=queue_name)
            ch.queue_declare(queue=queue_name, durable=True)
            return conn, ch
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"{e}")
        sys.exit(1)

def send_message(timestamp: str, temperature: float, queue_name: str):
    """Send a message to RabbitMQ"""
    try:
        conn, ch = rabbitmq_connection(host, queues)
        # create a message tuple
        message = f"{timestamp}, {temperature}"

        # publish the message to the queue
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        logger.info(f"[X] Sent to queue{queue_name}: {message}")
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
    finally: 
        conn.close()

def read_tasks(file_name):
    with open(file_name, 'r') as csv_file:
        reader = csv.reader(csv_file)
        rows = list(reader)  # Convert the reader to a list of rows

        if len(rows) > 1:  # Check if there are rows other than the header
            next(reader)  # Skip header row
            for row in rows[1:]:  # Iterate over the remaining rows
                # Process each row
                ...
        else:
            logger.warning(f"The file '{file_name}' is empty or has only a header row.")

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin sitepyton
    # Allow user to choose whether they would like to be directed to the Admin site 
    offer_rabbitmq_admin_site(show_offer=False)
    # create variables   
    file_name = 'smoker-temps.csv'
    host = "localhost"
    queues = ["01-smoker", "02-food-A", "02-food-B"]
    #send message to the queue
    read_tasks(file_name)