# PostgreSQL proxy connections

Multigateway can accept optional PROXY protocol v2 headers before PostgreSQL negotiation or TLS. These headers carry the original client address across a TCP proxy. The primary and replica listeners use the same configuration.

By default, Multigateway rejects PROXY headers. `--pg-trusted-proxy-cidrs` accepts a comma-separated list of trusted proxy networks in CIDR notation, which describes an IP address range. Multigateway tests the immediate TCP peer against these ranges before it reads a header.

```sh
multigateway \
  --pg-tls-cert-file=/certs/server.crt \
  --pg-tls-key-file=/certs/server.key \
  --pg-trusted-proxy-cidrs=10.0.0.10/32,fd00::10/128 \
  --pg-tls-offload
```

Replace the example addresses with the addresses of your trusted Envoy proxies. Restrict network access so that untrusted workloads cannot use those source addresses. Any trusted peer can claim another client address through a PROXY header.

`--pg-tls-offload` permits TLS without ALPN, the protocol name in the TLS handshake, after a trusted header supplies a client address. Use it when Envoy answers SSLRequest and forwards the TLS bytes directly. The flag requires TLS certificates and at least one trusted proxy range. A conflicting ALPN value remains invalid.

PROXY v2 does not say whether Envoy answered SSLRequest. The exception therefore covers both direct and negotiated clients forwarded by a trusted proxy. Keep this mode on an endpoint dedicated to PostgreSQL traffic.

Clients without a PROXY header retain their existing behavior. They can send a normal SSLRequest or a permitted plaintext startup. Direct TLS without a header still requires `postgresql` ALPN. `LOCAL` and `UNSPEC` headers retain the TCP peer address and do not enable the ALPN exception.

The parser accepts TCP addresses for IPv4 and IPv6. It consumes optional metadata fields but does not use them for authentication. The authentication deadline covers the header, TLS handshake, and login. Malformed or untrusted headers close the connection without a plaintext PostgreSQL reply.

After a valid address header, `Conn.RemoteAddr()` returns the original client IP and port, including after TLS starts. Authentication logs include this address in `client_addr`. This change does not add IP allowlists or automatic bans.

The equivalent environment variables are `MT_PG_TRUSTED_PROXY_CIDRS` and `MT_PG_TLS_OFFLOAD`. Use command-line flags for the comma-separated CIDR list. The [PROXY protocol specification](https://www.haproxy.org/download/2.9/doc/proxy-protocol.txt) defines the header format and trust model.
