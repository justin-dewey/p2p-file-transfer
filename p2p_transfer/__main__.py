import argparse
import json
import logging
import socket
import sys
import time

from .discovery import DiscoveryServer, discover
from .protocol import ProtocolError
from .transfer import TransferServer, send_file


def main():
    parser = argparse.ArgumentParser(description='Direct LAN file transfers')
    sub = parser.add_subparsers(dest='command', required=True)
    serve = sub.add_parser('serve', help='Receive files and answer discovery queries')
    serve.add_argument('--directory', default='received')
    serve.add_argument('--host', default='0.0.0.0')
    serve.add_argument('--port', type=int, default=5001)
    serve.add_argument('--discovery-port', type=int, default=5002)
    serve.add_argument('--name', default=socket.gethostname())
    serve.add_argument('--workers', type=int, default=4)
    find = sub.add_parser('discover', help='List listening peers as JSON')
    find.add_argument('--timeout', type=float, default=2)
    find.add_argument('--broadcast', default='255.255.255.255')
    find.add_argument('--discovery-port', type=int, default=5002)
    send = sub.add_parser('send', help='Send a file to a discovered or known address')
    send.add_argument('file')
    send.add_argument('--host', required=True)
    send.add_argument('--port', type=int, default=5001)
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    try:
        if args.command == 'discover':
            print(json.dumps(discover(args.timeout, [(args.broadcast, args.discovery_port)]), indent=2))
        elif args.command == 'send':
            print(json.dumps(send_file(args.file, args.host, args.port), indent=2))
        else:
            server = TransferServer(args.directory, args.host, args.port, args.workers).start()
            discovery = None
            try:
                discovery = DiscoveryServer(server.port, args.name, args.host,
                                            args.discovery_port).start()
                print(json.dumps(dict(status='listening', port=server.port,
                                      discovery_port=discovery.port)), flush=True)
                while True:
                    time.sleep(0.5)
            finally:
                if discovery:
                    discovery.close()
                server.close()
    except KeyboardInterrupt:
        return 0
    except (OSError, ValueError, ProtocolError) as exc:
        print('Error: ' + str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
