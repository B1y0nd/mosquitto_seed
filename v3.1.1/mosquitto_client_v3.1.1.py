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

    def create_mqtt_packet(self, msg_type, variable_header, payload):
        remaining_length = len(variable_header) + len(payload)
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
        return fixed_header + variable_header + payload

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

        variable_header = struct.pack("!H4sBBH", 0x0004, b'MQTT', 0x04, 0x02, 60)
        payload = struct.pack("!H" + str(len(self.client_id)) + "s", len(self.client_id), self.client_id.encode())
        connect_packet = self.create_mqtt_packet(0x10, variable_header, payload)
        print("connect_packet:", connect_packet)
        self.sock.send(connect_packet)
        self.wait_for_response()

    def subscribe(self, topic, qos):
        packet_id = self.get_next_packet_id()
        variable_header = struct.pack("!H", packet_id)
        payload = struct.pack("!H" + str(len(topic)) + "sB", len(topic), topic.encode(), qos)
        subscribe_packet = self.create_mqtt_packet(0x82, variable_header, payload)
        print("subscribe_packet:", subscribe_packet)
        self.sock.send(subscribe_packet)
        self.wait_for_response()

    def unsubscribe(self, topic):
        packet_id = self.get_next_packet_id()
        variable_header = struct.pack("!H", packet_id)
        payload = struct.pack("!H" + str(len(topic)) + "s", len(topic), topic.encode())
        unsubscribe_packet = self.create_mqtt_packet(0xA2, variable_header, payload)
        print("unsubscribe_packet:", unsubscribe_packet)
        self.sock.send(unsubscribe_packet)
        self.wait_for_response()

    def publish(self, topic, message, qos):
        retain_flag = 0x00
        dup_flag = 0x00
        msg_type = 0x30 | (dup_flag << 3) | (qos << 1) | retain_flag

        topic_bytes = topic.encode()
        variable_header = struct.pack("!H", len(topic_bytes)) + topic_bytes

        if qos > 0:
            packet_id = self.get_next_packet_id()
            variable_header += struct.pack("!H", packet_id)

        payload = message.encode()
        publish_packet = self.create_mqtt_packet(msg_type, variable_header, payload)
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
mqtt_client.subscribe('test/topic1', 0)
mqtt_client.subscribe('test/topic2', 0)
mqtt_client.publish('test/topic1', 'Hello, MQTT!', 0)
mqtt_client.publish('test/topic2', 'Hello, Java!', 0)
mqtt_client.unsubscribe('test/topic1')
mqtt_client.publish('test/topic1', 'Hello, MQTT! Again!', 0)
mqtt_client.ping()
mqtt_client.disconnect()
