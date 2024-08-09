import paho.mqtt.client as mqtt

# MQTT broker settings
BROKER_IP = "159.89.103.242"
PORT = 1883
TOPIC = "battery_scada"

# Create a new MQTT client instance
client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")

# Set the on_connect callback function
client.on_connect = on_connect

def connect_client():
    client.connect(BROKER_IP, PORT, 60)
    client.loop_start()  # Start the loop to process network traffic

def publish_message(payload):
    client.publish(TOPIC, payload)
    print(f"Message '{payload}' sent to topic '{TOPIC}'")

def disconnect_client():
    client.loop_stop()  # Stop the loop
    client.disconnect()
