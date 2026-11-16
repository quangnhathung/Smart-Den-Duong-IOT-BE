from types import SimpleNamespace

from smartlight import transport as module
from smartlight.transport import MqttTransport


class Stop:
    def __init__(self):
        self.flag = False
        self.delays = []

    def is_set(self):
        return self.flag

    def set(self):
        self.flag = True

    def wait(self, delay):
        self.delays.append(delay)
        return self.flag


def test_fresh_paho_instance_per_session_no_outgoing_replay(monkeypatch):
    transport = MqttTransport("127.0.0.1", 1883)
    stop = Stop()
    transport._stop = stop
    instances = []
    deliveries = []
    transport.sink = lambda *args: deliveries.append(args)
    reason = SimpleNamespace(is_failure=False)

    class Client:
        def __init__(self, *args, **kwargs):
            assert kwargs["clean_session"] is True and kwargs["reconnect_on_failure"] is False
            self.packets = []
            self.number = len(instances)
            instances.append(self)

        def max_queued_messages_set(self, count):
            assert count == 8

        def connect(self, host, port, keepalive):
            assert (host, port, keepalive) == ("127.0.0.1", 1883, 15)
            self.on_connect(self, None, None, reason, None)

        def subscribe(self, topics):
            assert len(topics) == 3 and all("/diagnostics" not in topic for topic, _qos in topics)
            self.on_subscribe(self, None, 1, [reason] * 3, None)
            return module.mqtt.MQTT_ERR_SUCCESS, 1

        def publish(self, topic, payload, qos, retain):
            assert qos == 1 and retain is False
            self.packets.append((topic, payload))
            return SimpleNamespace(rc=module.mqtt.MQTT_ERR_SUCCESS)

        def loop(self, timeout):
            assert timeout == 0.1
            if self.number == 0:
                assert transport.publish("command", "old", transport.session)
                return module.mqtt.MQTT_ERR_CONN_LOST
            assert not self.packets
            self.on_message(self, None, SimpleNamespace(topic="availability", payload=b"{}"))
            stop.set()
            return module.mqtt.MQTT_ERR_SUCCESS

        def disconnect(self):
            return module.mqtt.MQTT_ERR_SUCCESS

    monkeypatch.setattr(module.mqtt, "Client", Client)
    transport._run()
    assert len(instances) == 2 and instances[0] is not instances[1]
    assert instances[0].packets == [("command", "old")] and instances[1].packets == []
    assert len(deliveries) == 1 and not transport.connected
    assert not transport.publish("command", "stale", "old-session")
