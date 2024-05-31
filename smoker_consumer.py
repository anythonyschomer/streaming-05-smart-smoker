# smoker_consumer.py
import pika

# Define the callback function for the smoker queue
def callback_smoker(ch, method, properties, body):
    print(f"Smoker Temperature: {body.decode()}")

# Establish a connection to the RabbitMQ server
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Declare the smoker queue
channel.queue_declare(queue='01-smoker', durable=True)

# Set up the consumer
channel.basic_consume(queue='01-smoker',
                      auto_ack=True,
                      on_message_callback=callback_smoker)

print('Waiting for messages. To exit, press CTRL+C')

# Start consuming messages
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()

# Close the connection
connection.close()