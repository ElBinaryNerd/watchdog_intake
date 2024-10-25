import pulsar
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class PulsarProducer:
    def __init__(self):
        """
        Initializes the Pulsar producer with a connection to the Pulsar broker using environment variables.
        """
        try:
            # Get Pulsar connection details from environment variables
            pulsar_host = os.getenv('PULSAR_HOST', 'localhost')
            pulsar_port = os.getenv('PULSAR_PORT', '6650')
            pulsar_topic = os.getenv('DOMAIN_TOPIC', 'delete-me-topic')
            topic = os.getenv('PULSAR_TOPIC', f"persistent://public/default/{pulsar_topic}")

            # Construct the Pulsar URL from host and port
            pulsar_url = f'pulsar://{pulsar_host}:{pulsar_port}'

            # Create a Pulsar client
            self.client = pulsar.Client(pulsar_url)

            # Create a producer on the specified topic
            self.producer = self.client.create_producer(topic)

            print(f"Pulsar Producer: {self.producer}")

        except Exception as e:
            print(f"Error initializing Pulsar Producer: {e}")
            raise

    def send(self, message):
        """
        Sends a message to the Pulsar topic.
        :param message: The message to send.
        """
        try:
            # Send the message to the Pulsar topic
            self.producer.send(message.encode('utf-8'))
            # print(f"Message sent to Pulsar: {message}")

        except Exception as e:
            print(f"Error sending message to Pulsar: {e}")

    def close(self):
        """
        Closes the Pulsar client and producer.
        """
        try:
            if self.producer:
                self.producer.close()
            if self.client:
                self.client.close()

        except Exception as e:
            print(f"Error closing Pulsar Producer: {e}")
