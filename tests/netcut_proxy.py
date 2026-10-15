#!/usr/bin/env python3
"""TCP proxy used to simulate network loss between device and cloud.
  python3 netcut_proxy.py LISTEN_PORT TARGET_HOST TARGET_PORT CUTFILE
While CUTFILE exists, existing connections are dropped and new ones are reset."""
import os
import socket
import sys
import threading
import time

lp, th, tp, cut = int(sys.argv[1]), sys.argv[2], int(sys.argv[3]), sys.argv[4]
conns = []


def pipe(a, b):
    try:
        while True:
            d = a.recv(65536)
            if not d or os.path.exists(cut):
                break
            b.sendall(d)
    except OSError:
        pass
    for s in (a, b):
        try:
            s.close()
        except OSError:
            pass


def killer():
    while True:
        if os.path.exists(cut):
            for c in list(conns):
                try:
                    c.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            conns.clear()
        time.sleep(0.1)


threading.Thread(target=killer, daemon=True).start()
srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", lp))
srv.listen(64)
while True:
    c, _ = srv.accept()
    if os.path.exists(cut):
        # behave like a dead network: hold the connection open silently (client times out)
        threading.Thread(target=lambda c=c: (time.sleep(10), c.close()), daemon=True).start()
        continue
    try:
        u = socket.create_connection((th, tp), timeout=3)
    except OSError:
        c.close()
        continue
    conns += [c, u]
    threading.Thread(target=pipe, args=(c, u), daemon=True).start()
    threading.Thread(target=pipe, args=(u, c), daemon=True).start()
