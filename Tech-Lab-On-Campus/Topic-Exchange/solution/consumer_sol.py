import pika
import os
from consumer_interface import mqConsumerInterface


class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        # Save parameters to class variables
        self.binding_key = binding_key
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.setupRMQConnection()
    
    def setupRMQConnection(self):
        # Establish connection to the RabbitMQ service
        self.conParams = pika.URLParameters(os.environ['AMQP_URL'])
        self.connection = pika.BlockingConnection(parameters=self.conParams)
        self.channel = self.connection.channel()

        # Declare a queue and exchange
        self.channel.exchange_declare(self.exchange_name, exchange_type="topic")
        self.channel.queue_declare(self.queue_name)

        # Bind the binding key to the queue on the exchange
        self.channel.queue_bind(self.queue_name, self.exchange_name, self.binding_key)

        # Finally set up a callback function for receiving messages
        self.channel.basic_consume(self.queue_name, self.on_message_callback, auto_ack=False)

    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
        # Print the UTF-8 string message
        self.channel.basic_ack(method_frame.delivery_tag, False)
        print(body)    

    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print(" [*] Waiting for messages. To exit press CTRL+C")
        # Start consuming messages
        self.channel.start_consuming()
    
    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")
        
        # Close Channel
        self.channel.close()

        # Close Connection
        self.connection.close()
        
