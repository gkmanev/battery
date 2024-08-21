import paho.mqtt.client as mqtt


class MqttClient:
    def __init__(self, broker, port, topic) -> None:
        self.broker = broker
        self.port = port
        self.topic = topic
        self.client = mqtt.Client()
        # Set the on_connect callback function
        self.client.on_connect = self.on_connect


    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT broker at {self.broker}:{self.port}")
        else:
            print(f"Failed to connect with result code {rc}")

    def connect_client(self):
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()  # Start the loop to process network traffic

    def publish_message(self, payload):
        # Publish message to the specified topic
        self.client.publish(self.topic, payload)
        print(f"Message '{payload}' sent to topic '{self.topic}'")

    def disconnect_client(self):
        self.client.loop_stop()  # Stop the loop
        self.client.disconnect()