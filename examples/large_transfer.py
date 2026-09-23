"""Optional bounded-memory local exercise; run from the repository root."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import time
import tracemalloc

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from p2p_transfer.transfer import TransferServer, send_file
from p2p_transfer.protocol import CHUNK_SIZE, MAX_FILE_SIZE


def main():
    parser = argparse.ArgumentParser(description='Stream and independently verify large files locally')
    parser.add_argument('--mib', type=int, default=64)
    parser.add_argument('--concurrent', type=int, default=2)
    args = parser.parse_args()
    size = args.mib * 1024**2
    if not 0 < size <= MAX_FILE_SIZE or not 1 <= args.concurrent <= 16:
        parser.error('Use 1–10240 MiB and 1–16 concurrent transfers')
    # Temporary source is sparse on supporting filesystems; received copies use real space.
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source = root / 'large.bin'
        with source.open('wb') as output:
            output.truncate(size)
        server = TransferServer(root / 'received', '127.0.0.1', 0,
                                workers=args.concurrent).start()
        tracemalloc.start()
        start = time.monotonic()
        try:
            with ThreadPoolExecutor(max_workers=args.concurrent) as pool:
                results = list(pool.map(lambda _: send_file(source, '127.0.0.1', server.port), range(args.concurrent)))
            for result in results:
                digest = hashlib.sha256()
                with (root / 'received' / result['name']).open('rb') as received:
                    while block := received.read(CHUNK_SIZE):
                        digest.update(block)
                assert digest.hexdigest() == result['sha256']
                assert (root / 'received' / result['name']).stat().st_size == size
            _, peak = tracemalloc.get_traced_memory()
            print(json.dumps(dict(bytes_per_file=size, concurrent=args.concurrent,
                elapsed_seconds=round(time.monotonic() - start, 3),
                peak_python_allocations_mib=round(peak / 1024**2, 2),
                independently_verified=True), indent=2))
        finally:
            tracemalloc.stop()
            server.close()


if __name__ == '__main__':
    main()
