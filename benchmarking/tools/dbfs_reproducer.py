#!/usr/bin/env python3
"""
Simple framed-TCP client for dbfs server.

Usage:
  python3 dbfs_reproducer.py --host dbfs --port 25432 --file request.json
  or: echo '{"sql": "...", "parameters": [...]}' | python3 dbfs_reproducer.py --host localhost --port 25432

The script sends a 4-byte big-endian length prefix followed by the JSON payload,
then reads a 4-byte length prefix and response payload.
"""

import argparse
import json
import socket
import struct
import sys


def recvall(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def send_request(host, port, payload, timeout=30):
    data = payload.encode("utf-8")
    length_prefix = struct.pack('!I', len(data))
    with socket.create_connection((host, port), timeout=5) as sock:
        sock.settimeout(timeout)
        sock.sendall(length_prefix + data)
        # read 4-byte length prefix
        raw = recvall(sock, 4)
        if raw is None:
            raise RuntimeError('EOF while reading length prefix')
        resp_len = struct.unpack('!I', raw)[0]
        resp = recvall(sock, resp_len)
        if resp is None:
            raise RuntimeError('EOF while reading response')
        return resp.decode('utf-8')


def main():
    parser = argparse.ArgumentParser(description='dbfs framed-TCP reproducer')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=25432)
    parser.add_argument('--file', help='JSON file containing request payload. If omitted, read from stdin.')
    parser.add_argument('--timeout', type=int, default=30)
    args = parser.parse_args()

    if args.file:
        with open(args.file, 'r', encoding='utf-8') as f:
            payload = f.read()
    else:
        payload = sys.stdin.read()
    payload = payload.strip()
    if not payload:
        print('No payload provided', file=sys.stderr)
        sys.exit(2)

    try:
        json.loads(payload)
    except Exception as e:
        print('Invalid JSON payload:', e, file=sys.stderr)
        sys.exit(2)

    print(f'Sending payload to {args.host}:{args.port}')
    try:
        resp = send_request(args.host, args.port, payload, timeout=args.timeout)
    except Exception as e:
        print('Error communicating with server:', e, file=sys.stderr)
        sys.exit(1)

    print('Response:')
    try:
        parsed = json.loads(resp)
        print(json.dumps(parsed, indent=2, ensure_ascii=False))
    except Exception:
        print(resp)


if __name__ == '__main__':
    main()
