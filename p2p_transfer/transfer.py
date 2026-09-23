"""Bounded streaming transfers. Each connection carries exactly one file."""
import hashlib
import logging
import os
from pathlib import Path
import socket
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

from .protocol import (CHUNK_SIZE, MAX_FILE_SIZE, VERSION, ProtocolError,
                       recv_exact, recv_frame, send_frame, validate_offer)

LOG = logging.getLogger(__name__)


def send_file(path, host, port=5001, timeout=30):
    path = Path(path)
    with path.open('rb') as source:
        size = os.fstat(source.fileno()).st_size
        offer = dict(type='offer', version=VERSION, name=path.name, size=size)
        validate_offer(offer)
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            send_frame(sock, offer)
            reply = recv_frame(sock)
            if reply.get('status') != 'ready':
                raise ProtocolError(reply.get('error', 'Receiver rejected transfer'))
            digest = hashlib.sha256()
            remaining = size
            while remaining:
                block = source.read(min(CHUNK_SIZE, remaining))
                if not block:
                    raise ProtocolError('Source file shrank during transfer')
                digest.update(block)
                sock.sendall(block)
                remaining -= len(block)
            if source.read(1):
                raise ProtocolError('Source file grew during transfer')
            sock.sendall(digest.digest())
            result = recv_frame(sock)
            if result.get('status') != 'ok' or result.get('sha256') != digest.hexdigest():
                raise ProtocolError(result.get('error', 'Invalid checksum acknowledgement'))
            return result


class TransferServer:
    def __init__(self, directory, host='0.0.0.0', port=5001, workers=4,
                 timeout=30, max_size=MAX_FILE_SIZE):
        if workers < 1:
            raise ValueError('workers must be positive')
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.timeout, self.max_size = timeout, max_size
        self.stop_event = threading.Event()
        self.slots = threading.BoundedSemaphore(workers)
        self.pool = ThreadPoolExecutor(max_workers=workers)
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.listener.bind((host, port))
            self.listener.listen(workers)
            self.listener.settimeout(0.2)
        except BaseException:
            self.listener.close()
            self.pool.shutdown()
            raise
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self._accept, name='tcp-listener')
        self.active = set()
        self.lock = threading.Lock()

    def start(self):
        self.thread.start()
        return self

    def close(self):
        self.stop_event.set()
        self.thread.join()
        self.listener.close()
        with self.lock:
            for sock in self.active:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        self.pool.shutdown(wait=True)

    def _accept(self):
        while not self.stop_event.is_set():
            try:
                sock, _ = self.listener.accept()
            except socket.timeout:
                continue
            sock.settimeout(self.timeout)
            if not self.slots.acquire(blocking=False):
                # Close immediately: no unbounded queue or blocking busy response.
                sock.close()
                continue
            with self.lock:
                self.active.add(sock)
            self.pool.submit(self._receive, sock)

    def _receive(self, sock):
        temporary = None
        try:
            name, size = validate_offer(recv_frame(sock), self.max_size)
            token = uuid.uuid4().hex
            destination = self.directory / (token + '_' + name)
            temporary = self.directory / (token + '.part')
            with temporary.open('xb') as output:
                send_frame(sock, {'status': 'ready'})
                remaining = size
                digest = hashlib.sha256()
                while remaining:
                    block = sock.recv(min(CHUNK_SIZE, remaining))
                    if not block:
                        raise ProtocolError('Disconnected during file body')
                    output.write(block)
                    digest.update(block)
                    remaining -= len(block)
                if recv_exact(sock, 32) != digest.digest():
                    raise ProtocolError('SHA-256 mismatch')
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, destination)
            temporary = None
            send_frame(sock, dict(status='ok', name=destination.name,
                                  size=size, sha256=digest.hexdigest()))
        except (OSError, ProtocolError) as exc:
            LOG.debug('Transfer failed: %s', exc)
            try:
                send_frame(sock, {'status': 'error', 'error': str(exc)})
            except OSError:
                pass
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            with self.lock:
                self.active.discard(sock)
            sock.close()
            self.slots.release()
