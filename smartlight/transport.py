"""MQTT3.1.1 transport. A disconnected Paho client is discarded, never reconnected."""

import logging
import threading
import time
from uuid import uuid4

import paho.mqtt.client as mqtt

from .models import PREFIX

LOG = logging.getLogger(__name__)


class MqttTransport:
    def __init__(self, host, port):
        self.host, self.port = host, port
        self._lock = threading.Lock()
        self._ready = False
        self._session = ""
        self._client = None
        self._stop = threading.Event()
        self._thread = None
        self.sink = None

    @property
    def connected(self):
        with self._lock:
            return self._ready

    @property
    def session(self):
        with self._lock:
            return self._session

    def start(self, sink):
        self.sink = sink
        self._thread = threading.Thread(target=self._run, name="mqtt-transport", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(5)
            if self._thread.is_alive():
                raise RuntimeError("MQTT transport did not stop within 5s")

    def publish(self, topic, payload, session):
        with self._lock:
            if not self._ready or session != self._session or self._client is None:
                return False
            return self._client.publish(topic, payload, qos=1, retain=False).rc == mqtt.MQTT_ERR_SUCCESS

    def _run(self):
        delay = 1
        while not self._stop.is_set():
            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"smartlight-backend-{uuid4().hex[:12]}",
                clean_session=True,
                protocol=mqtt.MQTTv311,
                reconnect_on_failure=False,
            )
            client.connect_timeout = 2
            client.max_queued_messages_set(8)
            session = str(uuid4())
            early_messages = []
            failed = threading.Event()

            def connected(c, _userdata, _flags, reason, _properties):
                if reason.is_failure:
                    failed.set()
                    return
                result, _mid = c.subscribe([(f"{PREFIX}/+/{kind}", 0) for kind in ("telemetry", "availability", "ack")])
                if result != mqtt.MQTT_ERR_SUCCESS:
                    failed.set()

            def subscribed(_c, _userdata, _mid, reasons, _properties):
                if not reasons or any(reason.is_failure for reason in reasons):
                    failed.set()
                    return
                with self._lock:
                    self._session, self._ready = session, True
                for topic, payload in early_messages:
                    self.sink(topic, payload, session)
                early_messages.clear()

            def message(_c, _userdata, value):
                payload = bytes(value.payload[:2049])
                if self.connected:
                    self.sink(value.topic, payload, session)
                elif len(early_messages) < 64:
                    early_messages.append((value.topic, payload))
                else:
                    failed.set()

            client.on_connect = connected
            client.on_subscribe = subscribed
            client.on_message = message
            with self._lock:
                self._client = client
                self._ready = False
            try:
                client.connect(self.host, self.port, keepalive=15)
                connect_deadline = time.monotonic() + 3
                while not self._stop.is_set() and not failed.is_set():
                    if client.loop(timeout=0.1) != mqtt.MQTT_ERR_SUCCESS:
                        break
                    if self.connected:
                        delay = 1
                    elif time.monotonic() >= connect_deadline:
                        break
            except (OSError, ValueError) as error:
                LOG.warning("MQTT connection failed: %s", error)
            finally:
                # Clear before disconnect: publish can no longer queue commands into this instance.
                with self._lock:
                    self._ready = False
                    self._client = None
                client.disconnect()
            if self._stop.wait(delay):
                return
            delay = min(delay * 2, 10)
