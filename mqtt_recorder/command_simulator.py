import paho.mqtt.client as mqtt
import threading
import logging
import json
from datetime import datetime, timezone

logger = logging.getLogger('CommandSimulator')


class CommandSimulator:

    def __init__(self, host: str, port: int, client_id: str, username: str,
                 password: str, ssl_context):
        self._site_sns: set = set()
        self._unit_sns: dict = {}       # unit_sn -> parent site_sn
        self._overrides: dict = {}      # topic -> (override_value, expiry_timer)
        self._lock = threading.Lock()

        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=client_id)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        if username is not None:
            self._client.username_pw_set(username, password)
        if ssl_context.enable:
            self._client.tls_set(ssl_context.ca_cert, ssl_context.certfile, ssl_context.keyfile)
            if ssl_context.tls_insecure:
                self._client.tls_insecure_set(True)
        self._client.connect(host=host, port=port)

    def start(self):
        self._client.loop_forever()

    def _on_connect(self, client, userdata, flags, rc):
        logger.info('CommandSimulator connected to broker')
        client.subscribe([('ems/site/#', 0), ('Fractal/+/CMD', 0)])

    def _on_message(self, client, userdata, msg):
        topic = msg.topic

        if topic.startswith('Fractal/') and topic.endswith('/CMD'):
            self._handle_command(topic, msg)
        else:
            self._update_topology(topic)
            self._intercept_override(topic, msg)

    def _update_topology(self, topic: str):
        parts = topic.split('/')
        # ems/site/{site_sn}/...
        if len(parts) < 3:
            return
        site_sn = parts[2]
        self._site_sns.add(site_sn)
        # ems/site/{site_sn}/unit/{unit_sn}/...
        if len(parts) >= 5 and parts[3] == 'unit':
            unit_sn = parts[4]
            self._unit_sns[unit_sn] = site_sn

    def _intercept_override(self, topic: str, msg):
        with self._lock:
            if topic not in self._overrides:
                return
            override_value, _ = self._overrides[topic]
        try:
            current = msg.payload.decode('utf-8')
        except UnicodeDecodeError:
            return
        if current != override_value:
            self._client.publish(topic, override_value, retain=True)

    def _handle_command(self, topic: str, msg):
        try:
            cmd_data = json.loads(msg.payload.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning('Failed to parse CMD payload on %s', topic)
            return

        device_id = cmd_data.get('deviceId', topic.split('/')[1])
        cmd = cmd_data.get('CMD', '')
        operator = cmd_data.get('Operator', 'Unknown')
        operator_name = operator.split('@')[0]

        now = datetime.now(timezone.utc)
        ts = now.strftime('%Y-%m-%dT%H:%M:%S.') + f'{now.microsecond // 1000:03d}Z'

        msg_payload = json.dumps({
            'MSG': f'{operator_name} sent command {cmd}',
            'SN': device_id,
            'TS': ts,
            'Description': 'MSG'
        })
        self._client.publish(f'MSG/{device_id}', msg_payload, retain=False)
        logger.info('MSG/%s: %s sent command %s', device_id, operator_name, cmd)

        if cmd.startswith('System Mode::Manual'):
            self._apply_override(device_id, 'Manual')
        elif cmd.startswith('System Mode::Auto'):
            self._apply_override(device_id, 'Auto')

    def _apply_override(self, device_id: str, value: str):
        if device_id in self._unit_sns:
            site_sn = self._unit_sns[device_id]
            topic = f'ems/site/{site_sn}/unit/{device_id}/root/RunMode/string'
        elif device_id in self._site_sns:
            topic = f'ems/site/{device_id}/root/LocRemCtl/string'
        else:
            logger.warning('Topology not yet discovered for device %s — override skipped', device_id)
            return
        self._set_override(topic, value)

    def _set_override(self, topic: str, value: str):
        with self._lock:
            existing = self._overrides.get(topic)
            if existing is not None:
                existing[1].cancel()
            self._client.publish(topic, value, retain=True)
            logger.info('Override set: %s = %s (expires in 5 min)', topic, value)
            timer = threading.Timer(300.0, self._expire_override, args=(topic,))
            timer.daemon = True
            timer.start()
            self._overrides[topic] = (value, timer)

    def _expire_override(self, topic: str):
        with self._lock:
            self._overrides.pop(topic, None)
        logger.info('Override expired: %s', topic)
