"""Wire format: four-byte network-order JSON length, followed by UTF-8 JSON."""
import json
import struct

VERSION = 1
MAX_FRAME = 16 * 1024
CHUNK_SIZE = 1024 * 1024
MAX_FILE_SIZE = 10 * 1024**3  # 10 GiB; also covers a decimal 10 GB file.


class ProtocolError(Exception):
    pass


def recv_exact(sock, count):
    data = bytearray()
    while len(data) < count:
        block = sock.recv(count - len(data))
        if not block:
            raise ProtocolError('Connection closed before message was complete')
        data.extend(block)
    return bytes(data)


def send_frame(sock, message):
    body = json.dumps(message, separators=(',', ':')).encode('utf-8')
    if len(body) > MAX_FRAME:
        raise ProtocolError('Metadata frame too large')
    sock.sendall(struct.pack('!I', len(body)) + body)


def recv_frame(sock):
    length = struct.unpack('!I', recv_exact(sock, 4))[0]
    if not 0 < length <= MAX_FRAME:
        raise ProtocolError('Invalid metadata frame length')
    try:
        message = json.loads(recv_exact(sock, length).decode('utf-8'))
    except (ValueError, UnicodeError) as exc:
        raise ProtocolError('Invalid JSON metadata') from exc
    if not isinstance(message, dict):
        raise ProtocolError('Metadata must be an object')
    return message


def validate_offer(message, limit=MAX_FILE_SIZE):
    if message.get('type') != 'offer' or message.get('version') != VERSION:
        raise ProtocolError('Unsupported protocol or message type')
    name, size = message.get('name'), message.get('size')
    # Use the same safe subset on macOS, Windows, and Linux.
    if (not isinstance(name, str) or not name or name in ('.', '..')
            or len(name.encode('utf-8')) > 180
            or any(ord(c) < 32 or c in '/\\<>:"|?*' for c in name)
            or name.endswith((' ', '.'))):
        raise ProtocolError('Unsafe file name')
    if type(size) is not int or not 0 <= size <= limit:
        raise ProtocolError('File size outside allowed range')
    return name, size
