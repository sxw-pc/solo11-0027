import http.client
import json
import socket
import threading

from werkzeug.serving import make_server

from lakewatch.web.app import create_app

LOOPBACK = {"127.0.0.1", "localhost", "::1"}


def test_service_starts_and_serves_without_dns_or_outbound_network(app_config, monkeypatch):
    app = create_app(app_config)
    server = make_server("127.0.0.1", 0, app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    original_getaddrinfo = socket.getaddrinfo
    original_connect = socket.socket.connect

    def guarded_getaddrinfo(host, *args, **kwargs):
        if host not in LOOPBACK:
            raise AssertionError(f"unexpected DNS lookup: {host}")
        return original_getaddrinfo(host, *args, **kwargs)

    def guarded_connect(sock, address):
        host = address[0] if isinstance(address, tuple) else address
        if host not in LOOPBACK:
            raise AssertionError(f"unexpected outbound connection: {host}")
        return original_connect(sock, address)

    monkeypatch.setattr(socket, "getaddrinfo", guarded_getaddrinfo)
    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    try:
        port = server.server_port
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        connection.request("GET", "/healthz")
        health = connection.getresponse()
        assert health.status == 200
        assert json.loads(health.read())["offline"] is True

        connection.request("GET", "/api/tables")
        tables = connection.getresponse()
        assert tables.status == 200
        payload = json.loads(tables.read())
        assert payload["summary"]["table_count"] == 4

        connection.request("GET", "/")
        page = connection.getresponse()
        html = page.read().decode("utf-8")
        assert page.status == 200
        assert "Lakewatch" in html
        assert "http://" not in html and "https://" not in html
        connection.close()
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
