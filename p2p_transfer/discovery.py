"""IPv4 LAN discovery. The reply source IP is used, never a claimed IP."""
import json
import socket
import threading
import time
import uuid

MAGIC = 'p2p-file-transfer-v1'
DISCOVERY_PORT = 5002


def encode(value):
    return json.dumps(value, separators=(',', ':')).encode('utf-8')


class DiscoveryServer:
    def __init__(self, tcp_port, name, host='0.0.0.0', port=DISCOVERY_PORT):
        if not name or len(name.encode('utf-8')) > 100:
            raise ValueError('Peer name must be 1–100 UTF-8 bytes')
        self.identity = dict(magic=MAGIC, type='peer', id=uuid.uuid4().hex,
                             name=name, port=tcp_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind((host, port))
            self.sock.settimeout(0.2)
        except BaseException:
            self.sock.close()
            raise
        self.port = self.sock.getsockname()[1]
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._run, name='udp-discovery')

    def start(self):
        self.thread.start()
        return self

    def close(self):
        self.stopped.set()
        self.thread.join()
        self.sock.close()

    def _run(self):
        while not self.stopped.is_set():
            try:
                data, address = self.sock.recvfrom(2048)
                query = json.loads(data)
                if (isinstance(query, dict) and query.get('magic') == MAGIC
                        and query.get('type') == 'discover'
                        and isinstance(query.get('nonce'), str)
                        and len(query['nonce']) == 32):
                    self.sock.sendto(encode(dict(self.identity, nonce=query['nonce'])), address)
            except (ValueError, OSError):
                continue


def discover(timeout=2, targets=None):
    if timeout <= 0:
        raise ValueError('Discovery timeout must be positive')
    targets = targets or [('255.255.255.255', DISCOVERY_PORT)]
    nonce = uuid.uuid4().hex
    query = encode(dict(magic=MAGIC, type='discover', nonce=nonce))
    peers = {}
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(('', 0))
        deadline = time.monotonic() + timeout
        next_send = 0
        while time.monotonic() < deadline:
            now = time.monotonic()
            if now >= next_send:
                for target in targets:
                    sock.sendto(query, target)
                next_send = now + 0.5
            sock.settimeout(max(0.001, min(deadline - now, next_send - now)))
            try:
                data, address = sock.recvfrom(2048)
                peer = json.loads(data)
                if (not isinstance(peer, dict) or peer.get('magic') != MAGIC
                        or peer.get('type') != 'peer' or peer.get('nonce') != nonce
                        or type(peer.get('port')) is not int
                        or not 1 <= peer['port'] <= 65535
                        or not isinstance(peer.get('id'), str)
                        or not isinstance(peer.get('name'), str)):
                    continue
                peers[(address[0], peer['port'])] = {
                    'id': peer['id'], 'name': peer['name'],
                    'host': address[0], 'port': peer['port']}
            except (ValueError, socket.timeout):
                continue
    return sorted(peers.values(), key=lambda p: (p['host'], p['port']))
