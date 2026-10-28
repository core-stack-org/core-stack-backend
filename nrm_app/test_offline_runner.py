import socket

import requests
from django.test import SimpleTestCase

from nrm_app.test_runner import NetworkBlockedError


class OfflineTestRunnerTests(SimpleTestCase):
    def test_blocks_remote_hosts(self):
        with self.assertRaises(NetworkBlockedError):
            socket.getaddrinfo("example.com", 443)
        with self.assertRaises(requests.exceptions.ConnectionError) as ctx:
            requests.get("https://example.com", timeout=5)
        self.assertIn("Tests must not use the network", str(ctx.exception))

    def test_allows_local_hosts(self):
        self.assertTrue(socket.getaddrinfo("localhost", 5432))
        self.assertTrue(socket.getaddrinfo("127.0.0.1", 5432))
