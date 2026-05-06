#!/usr/bin/env python3
"""
Test script for boot token processing.
Publishes a boot message to the MQTT topic that triggers the Lambda function,
then listens for the session token response.
"""

import argparse
import json
import random
import ssl
import threading
import time
import logging
import os

import paho.mqtt.client as mqtt

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BootTokenClient:
    """Client for publishing boot messages and receiving session tokens via MQTT."""

    def __init__(self, customer_id: str, application_id: str, device_serial: str,
                 device_type: str, version: str, git_hash: str, gateway_serial: str,
                 broker: str = "mqtt.ulogger.ai", port: int = 8883,
                 cert_file: str = "certificate.pem.crt", key_file: str = "private.pem.key"):
        """
        Initialize the boot token client.

        Args:
            customer_id: Customer identifier
            application_id: Application identifier
            device_serial: Device serial number
            device_type: Device type string
            version: Firmware version string (e.g. "v1.0.0")
            git_hash: Git commit hash for the firmware
            gateway_serial: Gateway serial
            broker: MQTT broker hostname
            port: MQTT broker port
            cert_file: Path to certificate file
            key_file: Path to private key file
        """
        self.customer_id = customer_id
        self.application_id = application_id
        self.device_serial = device_serial
        self.device_type = device_type
        self.version = version
        self.git_hash = git_hash
        self.gateway_serial= gateway_serial
        self.broker = broker
        self.port = port
        self.cert_file = cert_file
        self.key_file = key_file
        self.mqtt_client = None
        self.publish_complete = threading.Event()
        self.response_received = threading.Event()
        self.session_token = None

        if not os.path.exists(self.cert_file) or not os.path.exists(self.key_file):
            raise FileNotFoundError(f"Certificate or key file not found: {self.cert_file}, {self.key_file}")

    def setup_mqtt_client(self):
        """Set up MQTT client with certificate-based authentication."""
        try:
            self.mqtt_client = mqtt.Client(
                client_id=f"{self.gateway_serial}-{random.randint(1000, 999999)}"
            )

            self.mqtt_client.tls_set(
                ca_certs="certs/waites/AmazonRootRSA_CAs.pem",  # Set to CA file if required by broker
                certfile=self.cert_file,
                keyfile=self.key_file,
                tls_version=ssl.PROTOCOL_TLSv1_2
            )

            self.mqtt_client.on_connect = self._on_connect
            self.mqtt_client.on_publish = self._on_publish
            self.mqtt_client.on_message = self._on_message
            self.mqtt_client.on_disconnect = self._on_disconnect

            logger.info(f"Connecting to MQTT broker: {self.broker}:{self.port}")
            self.mqtt_client.connect(self.broker, self.port, 60)
            self.mqtt_client.loop_start()

        except Exception as e:
            logger.error(f"Failed to setup MQTT client: {e}")
            raise

    def _on_connect(self, client, userdata, flags, rc):
        """Callback for when MQTT client connects."""
        if rc == 0:
            logger.info("Connected to MQTT broker")
            # Subscribe to the response topic
            response_topic = f"ulogger/boot/v0/{self.customer_id}/{self.application_id}/{self.device_serial}"
            client.subscribe(response_topic, qos=1)
            logger.info(f"Subscribed to response topic: {response_topic}")
        else:
            logger.error(f"Failed to connect to MQTT broker, return code {rc}")

    def _on_publish(self, client, userdata, mid):
        """Callback for when a message is published."""
        logger.info(f"Message published successfully (mid: {mid})")
        self.publish_complete.set()

    def _on_message(self, client, userdata, msg):
        """Callback for when a response message is received."""
        try:
            payload = json.loads(msg.payload.decode('utf-8'))
            logger.info(f"Response received on topic '{msg.topic}': {payload}")
            self.session_token = payload.get('token')
            logger.info(f"Session token: {self.session_token}")
            self.response_received.set()
        except Exception as e:
            logger.error(f"Error parsing response message: {e}")

    def _on_disconnect(self, client, userdata, rc):
        """Callback for when MQTT client disconnects."""
        logger.info(f"Disconnected from MQTT broker, return code {rc}")

    def publish_boot_message(self, timestamp: int = None) -> bool:
        """
        Publish a boot message to the MQTT boot topic and wait for the session token response.

        Args:
            timestamp: Optional Unix epoch timestamp for historical ingestion.
                       Defaults to the current time if not provided.

        Returns:
            True if the message was published and a response was received successfully.
        """
        try:
            if self.mqtt_client is None:
                self.setup_mqtt_client()
                time.sleep(2)  # Give MQTT client time to connect and subscribe

            topic = f"ulogger/boot/v0/{self.customer_id}/{self.application_id}"

                        
            #"mqtt_topic": topic,
            payload = {
                #"mqtt_topic": topic,
                "device_type": self.device_type,
                "git": self.git_hash,
                "serial": self.device_serial,
                "version": self.version,
            }

            if timestamp is not None:
                payload["timestamp"] = timestamp

            message = json.dumps(payload)

            logger.info(f"Publishing to topic: {topic}")
            logger.info(f"Payload: {message}")

            self.publish_complete.clear()
            self.response_received.clear()

            result = self.mqtt_client.publish(topic, message, qos=1)

            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                logger.error(f"Failed to publish boot message: {result.rc}")
                return False

            if not self.publish_complete.wait(timeout=10):
                logger.warning("Timed out waiting for publish confirmation")

            logger.info("Boot message published, waiting for session token response...")
            if self.response_received.wait(timeout=15):
                logger.info(f"Boot token exchange complete. Session token: {self.session_token}")
                return True
            else:
                logger.warning("Timed out waiting for session token response")
                return False

        except Exception as e:
            logger.error(f"Error publishing boot message: {e}")
            return False

    def cleanup(self):
        """Clean up MQTT client connection."""
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            logger.info("MQTT client disconnected")


def main():
    """Main function for publishing a boot message and receiving a session token."""
    parser = argparse.ArgumentParser(description="Boot Token Test Client")
    parser.add_argument('--customer_id', type=str, required=True, help='Customer identifier')
    parser.add_argument('--application_id', type=str, required=True, help='Application identifier')
    parser.add_argument('--serial', type=str, required=True, help='Device serial number')
    parser.add_argument('--device_type', type=str, required=True, help='Device type')
    parser.add_argument('--version', type=str, required=True, help='Firmware version (e.g. v1.0.0)')
    parser.add_argument('--git_hash', type=str, required=True, help='Git commit hash for the firmware')
    parser.add_argument('--timestamp', type=int, default=None,
                        help='Optional Unix epoch timestamp for historical ingestion')
    parser.add_argument('--cert_path', type=str, default='certificate.pem.crt',
                        help='Path to certificate file')
    parser.add_argument('--key_path', type=str, default='private.pem.key',
                        help='Path to private key file')
    parser.add_argument('--broker', type=str, default='mqtt.ulogger.ai',
                        help='MQTT broker hostname')
    parser.add_argument('--port', type=int, default=8883, help='MQTT broker port')
    parser.add_argument('--gateway_serial', type=str, required=True, help='Gateway serial number')

    args = parser.parse_args()

    try:
        client = BootTokenClient(
            customer_id=args.customer_id,
            application_id=args.application_id,
            device_serial=args.serial,
            device_type=args.device_type,
            version=args.version,
            git_hash=args.git_hash,
            broker=args.broker,
            port=args.port,
            cert_file=args.cert_path,
            key_file=args.key_path,
            gateway_serial=args.gateway_serial,
        )

        success = client.publish_boot_message(timestamp=args.timestamp)

        if success:
            logger.info(f"Boot token test passed! Session token: {client.session_token}")
        else:
            logger.error("Boot token test failed!")

        client.cleanup()

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
