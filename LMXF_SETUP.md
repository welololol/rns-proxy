# LMXF Router Setup

This guide sets up one Reticulum exit node and a local multi-destination
router. Once the router is running, connections can select different RNS
destinations without restarting the proxy.

## 1. Prerequisites

You need:

- Rust and Cargo
- Reticulum and a running `rnsd` daemon on both machines
- Network connectivity between the Reticulum interfaces
- This repository checked out on the local machine and exit node

Build the binary:

```bash
cargo build --release
```

The binary is located at:

```text
target/release/rns-proxy
```

## 2. Start Reticulum

Start `rnsd` on each machine. The included example can be used during local
testing:

```bash
./examples/run_rnsd.sh
```

For a real deployment, configure Reticulum interfaces according to your
network. Both machines must be able to discover each other through Reticulum.

## 3. Start the remote exit node

On the machine that should make outbound connections, run:

```bash
target/release/rns-proxy server
```

The server prints a 32-character hexadecimal destination hash, for example:

```text
0123456789abcdef0123456789abcdef
```

Keep this hash. It identifies the remote node. The server identity is saved
under `~/.reticulum/rns_proxy_identity`, so the address remains stable across
restarts. To restrict the exposed ports, use `forward` instead of `server`:

```bash
target/release/rns-proxy forward -t 25565 -t 443
```

## 4. Start the local LMXF router

On the machine running your browser or Minecraft client:

```bash
target/release/rns-proxy router
```

By default it listens on:

```text
127.0.0.1:1080
```

To choose another address:

```bash
target/release/rns-proxy router --listen 127.0.0.1:1081
```

The router creates and caches one RNS link for every destination used. It also
reconnects links after temporary Reticulum failures.

## 5. Address format

For a service listening on the remote exit node itself, use:

```text
<destination-hash>.lmxf:<port>
```

Example for Minecraft:

```text
0123456789abcdef0123456789abcdef.lmxf:25565
```

This forwards to `127.0.0.1:25565` on the remote exit node.

To proxy a named host through the remote exit node, put the target hostname
before the destination:

```text
example.com.0123456789abcdef0123456789abcdef.lmxf:443
```

This asks the remote server to connect to `example.com:443`.

The URI form is available for bookmarks and integrations:

```text
rns://0123456789abcdef0123456789abcdef/localhost:25565
```

`rns://` is currently an address format understood by the proxy library; web
browsers and Minecraft do not natively handle that scheme.

## 6. Configure a browser

Configure the browser to use a SOCKS5 proxy at:

```text
Host: 127.0.0.1
Port: 1080
Type: SOCKS5
```

Enable remote DNS resolution through the SOCKS5 proxy if the browser offers
that option. This is usually called “Proxy DNS when using SOCKS v5”.

Then browse to an LMXF hostname, for example:

```text
https://example.com.0123456789abcdef0123456789abcdef.lmxf/
```

The remote web server must have a TLS certificate matching the hostname, or
the browser will display a certificate warning.

## 6a. Automatic Linux TUN mode

Linux users can avoid application SOCKS5 configuration entirely:

```bash
sudo target/release/rns-proxy tun
```

This creates the `rns0` interface, starts the internal LMXF router, and asks
`systemd-resolved` to send `.lmxf` DNS queries through the tunnel. Browsers,
Minecraft, and other applications can then use LMXF addresses normally.
It also installs the synthetic mapped-DNS route automatically.

The TUN mode requires `resolvectl` and root privileges. Stop it with `Ctrl-C`
to revert the interface DNS configuration.

## 7. Configure Minecraft Java

Configure Minecraft Java to use the local SOCKS5 proxy. One common method is
adding these JVM arguments to the launcher profile:

```text
-DsocksProxyHost=127.0.0.1 -DsocksProxyPort=1080
```

Use this as the multiplayer server address:

```text
0123456789abcdef0123456789abcdef.lmxf:25565
```

The remote exit node must have a Minecraft server listening on port `25565`,
or the `forward` command must explicitly allow that port.

## 8. Multiple destinations

Leave the router running and connect to different destinations independently:

```text
server-one-hash.lmxf:25565
server-two-hash.lmxf:25565
```

Each connection selects its destination from the hostname. No proxy restart is
required.

## Current limitations

- The TUN mode is currently Linux-first and requires root plus
  `systemd-resolved` for automatic split DNS.
- The SOCKS router remains available for systems without TUN support.
- Entering an `.lmxf` address without configuring the application to use
  `127.0.0.1:1080` will not route it automatically.
- UDP support depends on the application's SOCKS5 UDP-associate behavior.
- The remote server's filtering rules determine which ports and addresses are
  allowed.
