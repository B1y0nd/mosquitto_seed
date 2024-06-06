import socket
import struct
import select

class MQTTClient:
    def __init__(self, broker_host, broker_port, client_id):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id
        self.packet_id = 0
        self.sock = None

    def get_next_packet_id(self):
        self.packet_id += 1
        if self.packet_id > 0xFFFF:
            self.packet_id = 1
        return self.packet_id

    def create_mqtt_packet(self, msg_type, variable_header, payload, properties=b""):
        remaining_length = len(variable_header) + len(payload) + len(properties)
        remaining_length_bytes = []
        while True:
            encoded_byte = remaining_length % 128
            remaining_length = remaining_length // 128
            if remaining_length > 0:
                encoded_byte |= 0x80
            remaining_length_bytes.append(encoded_byte)
            if remaining_length == 0:
                break
        fixed_header = struct.pack("!B", msg_type) + bytes(remaining_length_bytes)
        return fixed_header + variable_header + properties + payload

    def wait_for_response(self, timeout=5):
        ready = select.select([self.sock], [], [], timeout)
        if ready[0]:
            try:
                response = self.sock.recv(4096)
                print("Received:", response)
            except BlockingIOError:
                print("No data received, continuing...")
            except Exception as e:
                print(f"Error: {e}")
        else:
            print("No data received within the timeout period")

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.broker_host, self.broker_port))
        self.sock.setblocking(0)  # Set the socket to non-blocking mode

        variable_header = struct.pack("!H4sBBH", 0x0004, b'MQTT', 0x05, 0x02, 60)
        payload = struct.pack("!H" + str(len(self.client_id)) + "s", len(self.client_id), self.client_id.encode())
        # Connect properties (Length (1 byte) + Session Expiry Interval (1 byte) + Session Expiry Interval value (4 bytes))
        connect_properties = b'\x08' + b'\x11' + struct.pack("!I", 0x0000012c) + b'\x22' + struct.pack("!H", 100)
        connect_packet = self.create_mqtt_packet(0x10, variable_header, payload, connect_properties)
        print("connect_packet:", connect_packet)
        self.sock.send(connect_packet)
        self.wait_for_response()

    def subscribe(self, topic, qos, subscription_identifier=None, user_properties=None):
        packet_id = self.get_next_packet_id()
        variable_header = struct.pack("!H", packet_id)

        subscribe_properties = b"" # Subscription Identifier
        if subscription_identifier is not None:
            subscribe_properties += struct.pack("!BB", 0x0B, subscription_identifier)
        user_property_bytes = b"" # User Property
        if user_properties is not None:
            for key, value in user_properties.items():
                user_property_bytes += struct.pack("!BH", 0x26, len(key)) + key.encode() + struct.pack("!H", len(value)) + value.encode()

        payload = struct.pack("!H" + str(len(topic)) + "sB", len(topic), topic.encode(), qos)
        subscribe_packet = self.create_mqtt_packet(0x82, variable_header, payload, struct.pack("!B", len(subscribe_properties) + len(user_property_bytes)) + subscribe_properties + user_property_bytes)
        print("subscribe_packet:", subscribe_packet)
        self.sock.send(subscribe_packet)
        self.wait_for_response()

    def unsubscribe(self, topic, user_properties=None):
        packet_id = self.get_next_packet_id()
        variable_header = struct.pack("!H", packet_id)

        user_property_bytes = b"" # User Property
        if user_properties is not None:
            for key, value in user_properties.items():
                user_property_bytes += struct.pack("!BH", 0x26, len(key)) + key.encode() + struct.pack("!H", len(value)) + value.encode()

        payload = struct.pack("!H" + str(len(topic)) + "s", len(topic), topic.encode())
        unsubscribe_packet = self.create_mqtt_packet(0xA2, variable_header, payload, struct.pack("!B",len(user_property_bytes)) + user_property_bytes)
        print("unsubscribe_packet:", unsubscribe_packet)
        self.sock.send(unsubscribe_packet)
        self.wait_for_response()

    def publish(self, topic, message, qos, topic_alias=None, user_properties=None):
        retain_flag = 0x00
        dup_flag = 0x00
        msg_type = 0x30 | (dup_flag << 3) | (qos << 1) | retain_flag

        variable_header = b""
        if topic:
            topic_bytes = topic.encode()
            variable_header += struct.pack("!H", len(topic_bytes)) + topic_bytes
        else:
            variable_header += struct.pack("!H", len(variable_header))

        if qos > 0:
            packet_id = self.get_next_packet_id()
            variable_header += struct.pack("!H", packet_id)

        publish_properties = b""
        publish_properties += struct.pack("!BB", 0x01, 0x01) # Payload Format Indicator
        publish_properties += struct.pack("!BI", 0x02, 3600) # Message Expiry Interval
        if topic_alias:
            publish_properties += struct.pack("!BH", 0x23, topic_alias) # Topic Alias
        if user_properties:
            for key, value in user_properties.items():
                publish_properties += struct.pack("!BH", 0x26, len(key)) + key.encode() + struct.pack("!H", len(value)) + value.encode()
        payload = message.encode()
        publish_packet = self.create_mqtt_packet(msg_type, variable_header, payload, struct.pack("!B", len(publish_properties)) + publish_properties)
        print("publish_packet:", publish_packet)
        self.sock.send(publish_packet)
        self.wait_for_response()

    def pubrel(self, packet_id):
        variable_header = struct.pack("!H", packet_id)
        pubrel_packet = self.create_mqtt_packet(0x62, variable_header, b"")
        print("pubrel_packet:", pubrel_packet)
        self.sock.send(pubrel_packet)
        self.wait_for_response()

    def ping(self):
        pingreq_packet = struct.pack("!BB", 0xc0, 0x00)
        print("pingreq_packet:", pingreq_packet)
        self.sock.send(pingreq_packet)
        self.wait_for_response()

    def disconnect(self):
        disconnect_packet = struct.pack("!BB", 0xe0, 0x00)
        print("disconnect_packet:", disconnect_packet)
        self.sock.send(disconnect_packet)
        self.wait_for_response()
        self.sock.close()

mqtt_client = MQTTClient('localhost', 1883, 'mqtt_client')
mqtt_client.connect()
mqtt_client.subscribe('test/topic1', 0, subscription_identifier=3, user_properties={"Country": "USA", "Region": "West"})
mqtt_client.subscribe('test/topic2', 0, subscription_identifier=4, user_properties={"Country": "EU", "Region": "Northern"})
mqtt_client.publish('test/topic1', 'Hello, MQTT!', 0, topic_alias = 1, user_properties={'priority': 'high', 'source': 'sensor1'})
mqtt_client.publish('test/topic2', 'Hello, Java!', 0, topic_alias = 2, user_properties={'priority': 'low', 'source': 'sensor2'})
mqtt_client.publish('', 'Hello, MQTT! Again!', 0, topic_alias = 1)
mqtt_client.unsubscribe('test/topic2', user_properties={"Country": "CN", "Region": "Eastern"})
mqtt_client.publish('', 'Hello, Java! Again!', 0, topic_alias = 2)
mqtt_client.ping()
mqtt_client.disconnect()
