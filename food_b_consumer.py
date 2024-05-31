# food_b_consumer.py
import pika

# Define the callback function for the food B queue
def callback_food_b(ch, method, properties, body):
    print(f"Food B Temperature: {body.decode()}")

# Establish a connection to the RabbitMQ server
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Declare the food B queue
channel.queue_declare(queue='03-food-B', durable=True)

# Set up the consumer
channel.basic_consume(queue='03-food-B',
                      auto_ack=True,
                      on_message_callback=callback_food_b)

print('Waiting for messages. To exit, press CTRL+C')

# Start consuming messages
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()

# Close the connection
connection.close()