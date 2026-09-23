# Cross-Platform P2P File Transfer

Readable Python 3.10+ project for direct file sharing on a trusted local network. Uses only the Python standard library. No cloud storage, relay server, API key, or runtime dependency. macOS, Linux, and Windows use the same source code.

## Quick start

On the receiving computer:

```bash
python3 -m p2p_transfer serve --name My-Mac --directory received
```

On another computer on the same LAN:

```bash
python3 -m p2p_transfer discover
python3 -m p2p_transfer send examples/message.txt --host 192.168.1.25
```

Replace `192.168.1.25` with the `host` returned by discovery. The successful JSON response includes the receiver's saved filename, byte count, and SHA-256 digest. Stop a receiver with Control-C. Each device can run a receiver and launch send commands, making every device a peer.

## Design

1. **UDP discovery:** a client broadcasts a small JSON discovery request on UDP port 5002. Listening peers reply to the request's source address with their name and TCP port. A random request nonce correlates replies. Discovery repeats the request every half second and deduplicates replies by address and port. The client uses the actual reply source IP.
2. **Direct TCP transfer:** the sender connects to the selected peer on TCP port 5001. TCP supplies ordered, reliable bytes, but has no message boundaries, so the application supplies framing.
3. **Metadata framing:** each JSON control message has a four-byte unsigned big-endian byte length followed by UTF-8 JSON. Frames are limited to 16 KiB. The offer includes protocol version, filename, and exact file size.
4. **Chunked streaming:** after the receiver accepts the offer, the sender reads and sends at most 1 MiB at a time. The receiver consumes exactly the advertised number of bytes. Chunk boundaries need not match TCP reads. SHA-256 is updated incrementally on both ends.
5. **Verification and commit:** the sender appends a raw 32-byte SHA-256 digest. The receiver compares it with its own digest, flushes and syncs the temporary file, then atomically renames it. It acknowledges the saved filename, size, and hex digest. The sender checks the acknowledgement digest too.
6. **Multithreading:** a listener dispatches work to a fixed-size thread pool (four receivers by default). A semaphore limits admitted connections without an unbounded work queue. Socket and file I/O release the Python interpreter lock, so threads can overlap I/O. Separate send processes or callers can send concurrently.

## Wire protocol, version 1

```text
Sender                                      Receiver
  |---- frame {type:offer, version:1,           |
  |            name:..., size:N} ------------->|
  |<--- frame {status:ready} ------------------|
  |---- exactly N raw file bytes ------------>|
  |---- 32 raw SHA-256 digest bytes ---------->|
  |<--- frame {status:ok, name:...,             |
  |            size:N, sha256:hex} ------------|
```

Errors use `{ "status": "error", "error": "reason" }` when a connection still permits a reply. At capacity, new connections are closed immediately; retry after active transfers finish. One file uses one connection. Invalid or oversized metadata is rejected before a body is accepted.

## Large files and resource limits

The size limit is **10 GiB (10,737,418,240 bytes) per file**, which includes decimal 10 GB files. Python integers and the JSON size field avoid a 32-bit file-length limit. The four-byte frame length applies only to metadata.

Memory is O(active transfers × chunk size), not O(file size). Python allocations, thread stacks, socket buffers, and OS filesystem cache contribute separately; 1 MiB chunks do not mean exactly 1 MiB process memory per transfer. Four concurrent maximum-size files can consume about 40 GiB of disk. Ensure enough free space and a destination filesystem supporting files larger than 4 GB. There is no disk quota or reservation; disk-write failures abort the upload.

Socket operations have a 30-second inactivity timeout, not a 30-second total transfer limit. There is no overall transfer deadline. Don't edit source files during a transfer: size changes are detected, but same-size edits can produce a mixed snapshot that still verifies as the bytes actually sent.

## Run checks

```bash
python3 -m compileall -q p2p_transfer tests examples
python3 -m unittest discover -s tests -v
python3 examples/large_transfer.py --mib 64 --concurrent 2
```

An optional full-size exercise, with enough disk space and time:

```bash
python3 examples/large_transfer.py --mib 10240 --concurrent 2
```

This creates a sparse zero-filled source where supported, streams real bytes through loopback TCP, independently rehashes received files, and deletes temporary files afterward. Two 10 GiB destinations require 20 GiB; allow 30 GiB plus headroom if the source is not sparse. The script reports tracked Python allocations, **not total process RSS**. Zero-filled loopback results are correctness checks, not real-world network benchmarks.

## Scope and operational behavior

- Intended for trusted LANs: transfers are plaintext, with no authentication or encryption. SHA-256 detects mismatched bytes; it does not establish peer identity or prevent an attacker from replacing data and digest. Running a receiver allows reachable devices to upload into its destination directory.
- Discovery normally stays on one IPv4 broadcast domain. Guest Wi-Fi isolation, VPNs, firewalls, and routers can block it. Allow Python local network access and inbound TCP 5001 / UDP 5002 as appropriate. A direct known IP can bypass discovery but cannot bypass routing or firewall restrictions. There is no NAT traversal, relay, or automatic Internet reachability.
- `discover --broadcast 192.168.1.255` can target a subnet broadcast address when the default broadcast doesn't reach the intended interface. Use the address appropriate to your network.
- Received names get a random UUID prefix to avoid ordinary filename collisions and overwrites. Path separators, traversal names, control characters, and nonportable characters are rejected. Names are limited to 180 UTF-8 bytes. Use a destination directory controlled by you.
- Graceful cancellation, disconnects, checksum failures, and handled I/O errors clean up partial files. A hard process kill or power loss can leave `.part` files; remove those after stopping the receiver. A lost final acknowledgement can leave a verified saved file even though the sender reports failure. Retrying creates another copy.
- No resume, compression, folder transfer, GUI, persistent peer catalog, or delivery deduplication. The code favors an explainable implementation. Future improvements could add authenticated TLS, resumable chunks, and user acceptance prompts.