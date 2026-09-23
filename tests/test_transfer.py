import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from p2p_transfer.discovery import DiscoveryServer, discover
from p2p_transfer.protocol import MAX_FILE_SIZE, ProtocolError, recv_frame, send_frame, validate_offer
from p2p_transfer.transfer import TransferServer, send_file

class TransferTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.a = TransferServer(self.root / 'a', '127.0.0.1', 0, timeout=1).start()
        self.b = TransferServer(self.root / 'b', '127.0.0.1', 0, timeout=1).start()
    def tearDown(self):
        self.a.close()
        self.b.close()
        self.temp.cleanup()
    def test_two_peers_discover_and_exchange(self):
        da = DiscoveryServer(self.a.port, 'Alice', '127.0.0.1', 0).start()
        db = DiscoveryServer(self.b.port, 'Bob', '127.0.0.1', 0).start()
        try:
            peers = discover(0.25, [('127.0.0.1', da.port), ('127.0.0.1', db.port)])
            self.assertEqual({p['name'] for p in peers}, {'Alice', 'Bob'})
            source = self.root / 'binary.bin'
            source.write_bytes(os.urandom(3 * 1024**2 + 71))
            expected = hashlib.sha256(source.read_bytes()).hexdigest()
            first = send_file(source, '127.0.0.1', self.b.port)
            second = send_file(self.root / 'b' / first['name'], '127.0.0.1', self.a.port)
            self.assertEqual(first['sha256'], expected)
            self.assertEqual(second['sha256'], expected)
            self.assertEqual(hashlib.sha256((self.root / 'a' / second['name']).read_bytes()).hexdigest(), expected)
        finally:
            da.close()
            db.close()
    def test_concurrent_same_name_and_empty(self):
        source = self.root / 'same.bin'
        source.write_bytes(os.urandom(2 * 1024**2 + 1))
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: send_file(source, '127.0.0.1', self.b.port), range(4)))
        self.assertEqual(len({r['name'] for r in results}), 4)
        for result in results:
            self.assertEqual((self.root / 'b' / result['name']).read_bytes(), source.read_bytes())
        source.write_bytes(b'')
        self.assertEqual(send_file(source, '127.0.0.1', self.a.port)['sha256'], hashlib.sha256(b'').hexdigest())
    def test_corrupt_and_interrupted(self):
        for corrupt in (True, False):
            with socket.create_connection(('127.0.0.1', self.b.port)) as sock:
                send_frame(sock, dict(type='offer', version=1, name='bad.bin', size=3))
                self.assertEqual(recv_frame(sock)['status'], 'ready')
                if corrupt:
                    sock.sendall(b'abc' + bytes(32))
                    self.assertIn('SHA-256', recv_frame(sock)['error'])
                else:
                    sock.sendall(b'a')
            deadline = time.monotonic() + 2
            while list((self.root / 'b').iterdir()) and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(list((self.root / 'b').iterdir()), [])
    def test_invalid_metadata(self):
        for name, size in [('../escape', 1), ('a\\b', 1), ('ok', -1), ('ok', True), ('ok', MAX_FILE_SIZE + 1)]:
            with socket.create_connection(('127.0.0.1', self.b.port)) as sock:
                send_frame(sock, dict(type='offer', version=1, name=name, size=size))
                self.assertEqual(recv_frame(sock)['status'], 'error')
        self.assertEqual(validate_offer(dict(type='offer', version=1, name='large.bin', size=MAX_FILE_SIZE))[1], MAX_FILE_SIZE)
    def test_framing(self):
        left, right = socket.socketpair()
        try:
            wire = b'{"status":"ready"}'
            for byte in struct.pack('!I', len(wire)) + wire:
                left.sendall(bytes([byte]))
            self.assertEqual(recv_frame(right), {'status': 'ready'})
            left.sendall(struct.pack('!I', 999999))
            with self.assertRaises(ProtocolError):
                recv_frame(right)
        finally:
            left.close()
            right.close()

class ProcessTests(unittest.TestCase):
    def test_two_independent_peers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            processes = []
            try:
                peers = []
                for name in ('Alice', 'Bob'):
                    p = subprocess.Popen([sys.executable, '-m', 'p2p_transfer', 'serve', '--host', '127.0.0.1', '--port', '0', '--discovery-port', '0', '--name', name, '--directory', str(root / name)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    processes.append(p)
                    peers.append(json.loads(p.stdout.readline()))
                source = root / 'example.txt'
                source.write_text('Independent processes, verified bytes.\n')
                for peer, name in zip(peers, ('Alice', 'Bob')):
                    found = subprocess.run([sys.executable, '-m', 'p2p_transfer', 'discover', '--broadcast', '127.0.0.1', '--discovery-port', str(peer['discovery_port']), '--timeout', '0.2'], check=True, capture_output=True, text=True, timeout=5)
                    self.assertEqual(len(json.loads(found.stdout)), 1)
                    sent = subprocess.run([sys.executable, '-m', 'p2p_transfer', 'send', str(source), '--host', '127.0.0.1', '--port', str(peer['port'])], check=True, capture_output=True, text=True, timeout=5)
                    result = json.loads(sent.stdout)
                    destination = root / name / result['name']
                    self.assertEqual(destination.read_bytes(), source.read_bytes())
                    self.assertEqual(result['sha256'], hashlib.sha256(destination.read_bytes()).hexdigest())
            finally:
                for p in processes:
                    p.send_signal(signal.SIGINT)
                    p.communicate(timeout=5)
