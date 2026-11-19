"""Test runner that blocks network access, so no test can depend on it."""

import ipaddress
import socket

from django.test.runner import DiscoverRunner


class NetworkBlockedError(OSError):
    pass


def _is_local(host):
    if host in (None, "", "localhost"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _blocked(host):
    raise NetworkBlockedError(
        f"Tests must not use the network: tried to reach {host!r}. "
        "Mock the call instead."
    )


class OfflineTestRunner(DiscoverRunner):
    """Allow only local connections (database, Redis) while tests run.

    The block covers running the tests only. Loading the test modules and
    the app's own startup (e.g. Earth Engine initialisation) stay as they
    were, so a blocked startup call can't slow every run down.
    """

    def run_suite(self, suite, **kwargs):
        real_getaddrinfo = socket.getaddrinfo
        real_connect = socket.socket.connect

        def getaddrinfo(host, *args, **kwargs):
            if not _is_local(host):
                _blocked(host)
            return real_getaddrinfo(host, *args, **kwargs)

        def connect(sock, address):
            # Unix sockets have a path as their address.
            if isinstance(address, tuple) and not _is_local(address[0]):
                _blocked(address[0])
            return real_connect(sock, address)

        socket.getaddrinfo = getaddrinfo
        socket.socket.connect = connect
        try:
            return super().run_suite(suite, **kwargs)
        finally:
            socket.getaddrinfo = real_getaddrinfo
            socket.socket.connect = real_connect
