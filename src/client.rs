//! RNS SOCKS5 client -- local SOCKS5 server that tunnels through RNS.
//!
//! Equivalent of `rns_socks_client.py`.
//!
//! The client:
//! 1. Starts an RNS node, waits for a path to the server destination.
//! 2. Creates an RNS link to the server.
//! 3. Listens on a local TCP port for SOCKS5 connections.
//! 4. For each SOCKS5 CONNECT, sends a CONNECT frame through the mux and
//!    relays data bidirectionally.
//! 5. Automatically reconnects when the link is lost.
//!
//! SOCKS5 protocol handling is delegated to `fast-socks5`.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use fast_socks5::server::states::CommandRead;
use fast_socks5::server::Socks5ServerProtocol;
use fast_socks5::util::target_addr::TargetAddr;
use fast_socks5::{ReplyError, Socks5Command};
use log::{debug, error, info, warn};
use rns_net::{LinkId, RnsNode};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, Notify};

use crate::address::parse_lmxf_host;
use crate::forwarding::PortType;
use crate::forwarding::{tcp_tunnel, udp_tunnel, ForwardedPort};
use crate::mux::MuxHandle;
use crate::relay::relay_bidirectional_udp_client_side;
use crate::{
    create_node, encode_connect_payload, ensure_path, recall_sig_pub, relay_bidirectional_tcp,
    Frame, FrameType, ProxyEvent,
};

pub async fn run_client(server_hex: &str, listen_addr: &str) {
    if let Some(destination) = decode_hash(server_hex).await {
        if let Some((mux, node, rx)) = connect_rns(destination).await {
            let reconnect_notify = reconnect_generator(mux.clone(), node, rx, destination).await;
            run_sockets_proxy_handling(listen_addr, mux.clone(), reconnect_notify).await;
        }
    }
}

/// Run one local SOCKS5 listener that can select an RNS destination per
/// request using `<service>.<destination>.lmxf` hostnames.
///
/// Routes are cached, so changing servers does not require restarting the
/// process. Applications must be configured to use this SOCKS5 listener.
pub async fn run_router(listen_addr: &str) {
    let listener = match TcpListener::bind(listen_addr).await {
        Ok(listener) => listener,
        Err(error) => {
            error!("Failed to bind LMXF router on {}: {}", listen_addr, error);
            return;
        }
    };

    let routes = Arc::new(tokio::sync::Mutex::new(
        HashMap::<[u8; 16], MuxHandle>::new(),
    ));
    info!("LMXF router listening on {}", listen_addr);

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(value) => value,
            Err(error) => {
                warn!("LMXF accept error: {}", error);
                continue;
            }
        };

        let routes = routes.clone();
        tokio::spawn(async move {
            let sid = {
                // Session ids are unique per route; the route mux owns the
                // actual counter once the destination is selected.
                0
            };
            handle_router_session(sid, stream, routes).await;
            debug!("LMXF connection from {} closed", peer);
        });
    }
}

/// Run Linux zero-configuration mode: a real TUN interface backed by the
/// existing multi-destination SOCKS router.
#[cfg(target_os = "linux")]
pub async fn run_tun(listen_addr: &str, interface: &str) {
    use std::os::fd::AsRawFd;
    use std::process::Command;

    let router_listen = listen_addr.to_string();
    tokio::spawn(async move { run_router(&router_listen).await });

    let mut config = tun::Configuration::default();
    config
        .tun_name(interface)
        .address((198, 18, 0, 1))
        .netmask((255, 255, 0, 0))
        .destination((198, 18, 0, 1))
        .mtu(8500)
        .up();
    config.platform_config(|platform| {
        platform.ensure_root_privileges(true);
    });

    let device = match tun::create(&config) {
        Ok(device) => device,
        Err(error) => {
            error!("Failed to create TUN interface {}: {}", interface, error);
            return;
        }
    };
    let tun_fd = device.as_raw_fd();
    let port = listen_addr
        .rsplit_once(':')
        .map(|(_, port)| port)
        .unwrap_or("1080");

    // Split DNS lets normal applications resolve `.lmxf` names without
    // changing their proxy settings. tun2socks's mapdns preserves the name
    // when it creates the internal SOCKS request.
    let _ = Command::new("resolvectl")
        .args(["dns", interface, "198.18.0.2"])
        .status();
    let _ = Command::new("resolvectl")
        .args(["domain", interface, "~lmxf"])
        .status();
    let _ = Command::new("ip")
        .args(["route", "replace", "240.0.0.0/4", "dev", interface])
        .status();

    let config_text = format!(
        "tunnel:\n  name: {interface}\n  mtu: 8500\n  ipv4: 198.18.0.1\n  ipv6: 'fc00::1'\n\nsocks5:\n  address: 127.0.0.1\n  port: {port}\n  udp: 'udp'\n\nmapdns:\n  address: 198.18.0.2\n  port: 53\n  network: 240.0.0.0\n  netmask: 240.0.0.0\n  cache-size: 10000\n",
    );

    info!("TUN mode active on {}", interface);
    let task = tokio::task::spawn_blocking(move || {
        let _device = device;
        tun2socks::main_from_str(&config_text, tun_fd)
    });

    tokio::select! {
        result = task => {
            if let Ok(Err(code)) = result {
                error!("TUN bridge stopped with error {}", code);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Stopping TUN mode");
            tun2socks::quit();
        }
    }

    let _ = Command::new("resolvectl")
        .args(["revert", interface])
        .status();
    let _ = Command::new("ip")
        .args(["route", "del", "240.0.0.0/4", "dev", interface])
        .status();
}

#[cfg(not(target_os = "linux"))]
pub async fn run_tun(_listen_addr: &str, _interface: &str) {
    error!("TUN mode is currently implemented for Linux only");
}

async fn handle_router_session(
    _sid: u32,
    stream: TcpStream,
    routes: Arc<tokio::sync::Mutex<HashMap<[u8; 16], MuxHandle>>>,
) {
    let proto = match Socks5ServerProtocol::accept_no_auth(stream).await {
        Ok(proto) => proto,
        Err(error) => {
            debug!("LMXF SOCKS5 handshake failed: {}", error);
            return;
        }
    };
    let (proto, command, target_addr) = match proto.read_command().await {
        Ok(value) => value,
        Err(error) => {
            debug!("LMXF SOCKS5 command failed: {}", error);
            return;
        }
    };

    let (host, port) = target_addr.clone().into_string_and_port();
    // tun2socks opens a UDP association with 0.0.0.0:0 before it knows the
    // destination of an individual datagram. LMXF destination selection is
    // hostname-based, so UDP associations are not routable through the
    // multi-destination router yet. Reject this expected bootstrap quietly.
    if command == Socks5Command::UDPAssociate && host == "0.0.0.0" && port == 0 {
        let _ = proto.reply_error(&ReplyError::CommandNotSupported).await;
        return;
    }
    let route = match parse_lmxf_host(&host, port) {
        Some(route) => route,
        None => {
            let _ = proto.reply_error(&ReplyError::GeneralFailure).await;
            warn!("LMXF router rejected non-LMXF target {}:{}", host, port);
            return;
        }
    };

    let destination = route.destination;
    let mux = {
        let routes_guard = routes.lock().await;
        if let Some(mux) = routes_guard.get(&destination) {
            mux.clone()
        } else {
            drop(routes_guard);
            let Some((mux, node, rx)) = connect_rns(destination).await else {
                let _ = proto.reply_error(&ReplyError::GeneralFailure).await;
                return;
            };
            let notify = reconnect_generator(mux.clone(), node, rx, destination).await;
            // Keep the reconnect task alive through its owned arguments. The
            // notification is intentionally not needed by the router listener.
            drop(notify);
            let mut routes_guard = routes.lock().await;
            routes_guard
                .entry(destination)
                .or_insert_with(|| mux.clone())
                .clone()
        }
    };

    if !mux.is_connected().await {
        let _ = proto.reply_error(&ReplyError::GeneralFailure).await;
        return;
    }
    let sid = mux.next_session_id().await;
    let session_rx = mux.register_session(sid).await;
    handle_socks5_command(sid, proto, command, route.target, mux, session_rx).await;
}

pub async fn run_client_forward(server_hex: &str, ports: Vec<ForwardedPort>) {
    if let Some(destination) = decode_hash(server_hex).await {
        if let Some((mux, node, rx)) = connect_rns(destination).await {
            let reconnect_notify = reconnect_generator(mux.clone(), node, rx, destination).await;

            for port in ports {
                match port.r#type {
                    PortType::Tcp => {
                        let notify = reconnect_notify.clone();
                        let mux = mux.clone();
                        tokio::spawn(async move { tcp_tunnel(mux, notify, port).await });
                    }
                    PortType::Udp => {
                        let notify = reconnect_notify.clone();
                        let mux = mux.clone();
                        tokio::spawn(async move { udp_tunnel(mux.clone(), notify, port).await });
                    }
                    // oh my boilerplate.
                    PortType::TcpUdp => {
                        let notify = reconnect_notify.clone();
                        let mux1 = mux.clone();
                        let v = port.clone();
                        tokio::spawn(async move { tcp_tunnel(mux1, notify, v).await });
                        let notify = reconnect_notify.clone();
                        let mux2 = mux.clone();
                        let v = port.clone();
                        tokio::spawn(async move { udp_tunnel(mux2, notify, v).await });
                    }
                }
            }
            let __: () = std::future::pending().await; // so main thread never ends.
                                                       // run_sockets_proxy_handling(listen_addr, mux.clone(), reconnect_notify).await;
        }
    };
}

pub async fn decode_hash(server_hex: &str) -> Option<[u8; 16]> {
    return match hex::decode(server_hex) {
        Ok(v) if v.len() == 16 => {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(&v);
            Some(arr)
        }
        _ => {
            error!("Invalid server address: must be 32 hex chars (16 bytes)");
            return None;
        }
    };
}

pub async fn connect_rns(
    server_dest: [u8; 16],
) -> Option<(MuxHandle, Arc<RnsNode>, UnboundedReceiver<ProxyEvent>)> {
    let (node, mut rx) = match create_node() {
        Ok(v) => v,
        Err(e) => {
            error!("{}", e);
            return None;
        }
    };

    let mux = MuxHandle::new(Arc::clone(&node));

    // Initial path + link establishment
    let sig_pub_bytes = wait_for_path(&node, &server_dest).await;

    if !establish_link(&node, &mux, &server_dest, sig_pub_bytes, &mut rx).await {
        return None;
    }

    return Some((mux, node, rx));
}

pub async fn reconnect_generator(
    mux: MuxHandle,
    node: Arc<RnsNode>,
    rx: UnboundedReceiver<ProxyEvent>,
    server_hash: [u8; 16],
) -> Arc<Notify> {
    let reconnect_notify = Arc::new(Notify::new());

    // Spawn event dispatch + reconnection task
    let mux_dispatch = mux.clone();
    let node_reconn = Arc::clone(&node);
    let reconnect_notify_clone = Arc::clone(&reconnect_notify);
    tokio::spawn(async move {
        dispatch_and_reconnect(
            mux_dispatch,
            node_reconn,
            server_hash,
            rx,
            reconnect_notify_clone,
        )
        .await;
    });

    return reconnect_notify;
}

/// Run the SOCKS5 client.
pub async fn run_sockets_proxy_handling(
    listen_addr: &str,
    mux: MuxHandle,
    reconnect_notify: Arc<Notify>,
) {
    // Start SOCKS5 listener
    let listener = match TcpListener::bind(listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind SOCKS5 listener on {}: {}", listen_addr, e);
            return;
        }
    };

    info!("started listener {}", listen_addr);

    // let buf = &mut [0u8 ;65536];
    // Accept SOCKS5 connections
    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, _addr) = match accept_result {
                    Ok(sa) => sa,
                    Err(e) => {
                        warn!("Accept error: {}", e);
                        continue;
                    }
                };
                info!("{}", _addr);

                if !mux.is_connected().await {
                    warn!("No RNS link, rejecting connection");
                    drop(stream);
                    continue;
                }

                let sid = mux.next_session_id().await;
                let session_rx = mux.register_session(sid).await;
                let mux_clone = mux.clone();

                tokio::spawn(async move {
                    handle_socks5_session(sid, stream, mux_clone, session_rx).await;
                });
            }
            _ = reconnect_notify.notified() => {
                // Link was re-established, just continue accepting
                info!("SOCKS5 ready: {}", listen_addr);
            }

        }
    }
}

/// Establish an RNS link, waiting for the LinkEstablished event.
///
/// On success, sets the link id on the mux and returns `true`.
async fn establish_link(
    node: &RnsNode,
    mux: &MuxHandle,
    dest_hash: &[u8; 16],
    sig_pub_bytes: [u8; 32],
    rx: &mut mpsc::UnboundedReceiver<ProxyEvent>,
) -> bool {
    info!("Establishing link...");
    let link_id = match node.create_link(*dest_hash, sig_pub_bytes) {
        Ok(id) => LinkId::from(id),
        Err(e) => {
            error!("Failed to create link: {:?}", e);
            return false;
        }
    };

    loop {
        let event = match rx.recv().await {
            Some(e) => e,
            None => {
                error!("Event channel closed");
                return false;
            }
        };

        match event {
            ProxyEvent::LinkEstablished {
                link_id: lid,
                rtt,
                is_initiator,
            } => {
                if lid == link_id && is_initiator {
                    info!("RNS link established (rtt={:.1}ms)", rtt * 1000.0);
                    mux.set_link_id(link_id).await;
                    return true;
                }
            }
            ProxyEvent::LinkClosed {
                link_id: lid,
                reason,
            } => {
                if lid == link_id {
                    error!("Link closed during setup: {:?}", reason);
                    return false;
                }
            }
            _ => {}
        }
    }
}

/// Event dispatch loop with automatic reconnection.
///
/// Reads RNS events, dispatches channel messages to sessions, and when the
/// link is lost, waits briefly and re-establishes it.
async fn dispatch_and_reconnect(
    mux: MuxHandle,
    node: Arc<RnsNode>,
    dest_hash: [u8; 16],
    mut rx: mpsc::UnboundedReceiver<ProxyEvent>,
    reconnect_notify: Arc<Notify>,
) {
    loop {
        // --- Dispatch phase: forward channel messages to sessions ---
        loop {
            let event = match rx.recv().await {
                Some(e) => e,
                None => {
                    error!("Event channel closed, shutting down");
                    return;
                }
            };

            match event {
                ProxyEvent::LinkData { link_id, data } => {
                    // A shared RNS node can receive data for other links
                    // owned by the same process. Only decode frames from the
                    // link associated with this mux; foreign link payloads
                    // are arbitrary bytes and would poison recv_buf.
                    if !mux.is_link(link_id).await {
                        debug!("Ignoring data from unrelated RNS link {}", link_id);
                        continue;
                    }
                    for frame in mux.receive_data(&data).await {
                        mux.dispatch(frame).await;
                    }
                }
                ProxyEvent::LinkClosed { link_id, reason } => {
                    if !mux.is_link(link_id).await {
                        debug!("Ignoring close for unrelated RNS link {}", link_id);
                        continue;
                    }
                    warn!("Connection lost (link={}, reason={:?})", link_id, reason);
                    mux.reset().await;
                    break; // Exit dispatch loop to reconnect
                }
                _ => {}
            }
        }

        // --- Reconnection phase ---
        let mut delay = 0u64;

        loop {
            info!("Reconnecting in {}s...", delay);
            tokio::time::sleep(Duration::from_secs(delay)).await;

            // Make sure path is still valid
            if !ensure_path(&node, &dest_hash, 15).await {
                warn!("Path not found, will retry...");
                delay = (delay * 2).min(30);
                continue;
            }

            // Refresh signing public key — it may have changed if the server
            // was restarted and re-announced with a new identity.
            let sig_pub_bytes = match recall_sig_pub(&node, &dest_hash) {
                Some(sig_pub) => sig_pub,
                None => {
                    warn!("Failed to recall identity, will retry...");
                    delay = (delay * 2).min(30);
                    continue;
                }
            };

            if establish_link(&node, &mux, &dest_hash, sig_pub_bytes, &mut rx).await {
                info!("Reconnected successfully");
                reconnect_notify.notify_one();
                break; // Back to dispatch phase
            }

            warn!("Reconnection failed, will retry...");
            delay = (delay * 2 + 1).min(30);
        }
    }
}

/// Handle a single SOCKS5 client session using fast-socks5.
async fn handle_socks5_session(
    sid: u32,
    stream: tokio::net::TcpStream,
    mux: MuxHandle,
    session_rx: mpsc::UnboundedReceiver<Frame>,
) {
    // --- SOCKS5 handshake via fast-socks5 ---
    let proto = match Socks5ServerProtocol::accept_no_auth(stream).await {
        Ok(p) => p,
        Err(e) => {
            debug!("[{}] SOCKS5 auth handshake failed: {}", sid, e);
            return;
        }
    };

    let (proto, cmd, target_addr) = match proto.read_command().await {
        Ok(result) => result,
        Err(e) => {
            debug!("[{}] SOCKS5 read_command failed: {}", sid, e);
            return;
        }
    };
    info!("hand shake");

    handle_socks5_command(sid, proto, cmd, target_addr, mux, session_rx).await;
}

async fn handle_socks5_command(
    sid: u32,
    proto: Socks5ServerProtocol<TcpStream, CommandRead>,
    cmd: Socks5Command,
    target_addr: TargetAddr,
    mux: MuxHandle,
    mut session_rx: mpsc::UnboundedReceiver<Frame>,
) {
    match cmd {
        Socks5Command::TCPConnect => {
            if let Some(stream) =
                handle_tcp_connect(sid, mux.clone(), &mut session_rx, proto, target_addr).await
            {
                relay_bidirectional_tcp(sid, stream, mux, session_rx).await
            }
        }
        Socks5Command::UDPAssociate => {
            if let Some((udp_stream, stream)) =
                handle_udp_connect(sid, mux.clone(), &mut session_rx, proto, target_addr).await
            {
                relay_bidirectional_udp_client_side(sid, udp_stream, stream, mux, session_rx).await;
            }
        }

        Socks5Command::TCPBind => {
            _ = proto.reply_error(&ReplyError::CommandNotSupported).await;
        } // I'll be real I don't know what tcp bind is actually for, so it can just be an error
    }
}

pub async fn connect_tcp_server_side(
    sid: u32,
    mux: MuxHandle,
    session_rx: &mut mpsc::UnboundedReceiver<Frame>,
    target_addr: TargetAddr,
) -> Result<(), String> {
    //
    // if let Some(socket_addr) = filter_and_convert(target_addr, None).await {

    let (host, port) = target_addr.clone().into_string_and_port();
    info!("[{}] -> {}:{} tcp", sid, host, port);
    // Send CONNECT frame through RNS
    let connect_payload = encode_connect_payload(&host, port, false);
    mux.send(FrameType::Connect, sid, connect_payload).await;

    // Wait for CONN_OK or CONN_ERR with timeout
    tokio::time::timeout(Duration::from_secs(60), async {
        while let Some(frame) = session_rx.recv().await {
            match frame.frame_type {
                FrameType::ConnectOk => return Ok(()),
                FrameType::ConnectErr => {
                    let reason = String::from_utf8_lossy(&frame.payload).to_string();
                    return Err(reason);
                }
                _ => continue,
            }
        }
        Err("channel closed".to_string())
    })
    .await
    .map_err(|_e| "Timeout".into())
    .flatten()

    // a.ok()

    // } else {
    // return None;
    // }
}

async fn handle_tcp_connect(
    sid: u32,
    mux: MuxHandle,
    session_rx: &mut mpsc::UnboundedReceiver<Frame>,
    proto: Socks5ServerProtocol<TcpStream, CommandRead>,
    target_addr: TargetAddr,
) -> Option<TcpStream> {
    // info!("udp test data: {:?}, {:?}",cmd, target_addr);

    let (host, port) = target_addr.clone().into_string_and_port();
    info!("[{}] -> {}:{} tcp", sid, host, port);

    let connect_result = connect_tcp_server_side(sid, mux.clone(), session_rx, target_addr).await;

    info!(
        "[{}] -> {}:{} connection result {:?}",
        sid, host, port, connect_result
    );

    if let Ok(_) = connect_result {
        // Reply to SOCKS5 client based on RNS connection result
        let dummy_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0);

        let stream = match connect_result {
            Ok(()) => {
                // Connection succeeded -- send SOCKS5 success reply
                match proto.reply_success(dummy_addr).await {
                    Ok(s) => s,
                    Err(e) => {
                        debug!("[{}] Failed to send SOCKS5 reply: {}", sid, e);
                        mux.send(FrameType::Close, sid, Vec::new()).await;
                        mux.drop_session(sid).await;
                        return None;
                    }
                }
            }
            Err(reason) => {
                warn!("[{}] Remote connect failed: {}", sid, reason);
                let _ = proto.reply_error(&ReplyError::GeneralFailure).await;
                mux.drop_session(sid).await;
                return None;
            }
        };

        _ = stream.set_nodelay(true); // slightly more overhead but this means slightly less delay and the tcp stream is less slightly to time out.

        info!("[{}] - > {}:{}, fully connected to", sid, host, port);

        return Some(stream);
    } else {
        return None;
    }
}

pub async fn udp_bind_connect(
    sid: u32,
    mux: MuxHandle,
    session_rx: &mut mpsc::UnboundedReceiver<Frame>,
    target_addr: TargetAddr,
) -> Result<(), String> {
    let (host, port) = target_addr.into_string_and_port();

    info!("[{}] -> {}:{} udp", sid, host, port);

    // Send CONNECT frame through RNS
    let connect_payload = encode_connect_payload(&host, port, true);
    mux.send(FrameType::Connect, sid, connect_payload).await;

    // Wait for CONN_OK or CONN_ERR with timeout
    tokio::time::timeout(Duration::from_secs(60), async {
        while let Some(frame) = session_rx.recv().await {
            match frame.frame_type {
                FrameType::ConnectOk => return Ok(()),
                FrameType::ConnectErr => {
                    let reason = String::from_utf8_lossy(&frame.payload).to_string();
                    return Err(reason);
                }
                _ => continue,
            }
        }
        Err("channel closed".to_string())
    })
    .await
    .map_err(|_e| "Timeout".into())
    .flatten()
}

async fn handle_udp_connect(
    sid: u32,
    mux: MuxHandle,
    mut session_rx: &mut mpsc::UnboundedReceiver<Frame>,
    proto: Socks5ServerProtocol<TcpStream, CommandRead>,

    target_addr: TargetAddr,
) -> Option<(UdpSocket, TcpStream)> {
    // info!("udp test data: {:?}, {:?}",cmd, target_addr);

    // Extract host and port from TargetAddr

    let connect_result = udp_bind_connect(sid, mux.clone(), &mut session_rx, target_addr).await;
    // Reply to SOCKS5 client based on RNS connection result

    let udp_stream = UdpSocket::bind(format!("0.0.0.0:0"))
        .await
        .expect("unable to get udp socket");
    let relay_port = udp_stream.local_addr().unwrap().port();
    let relay_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), relay_port);

    match connect_result {
        Ok(()) => {
            // Connection succeeded -- send SOCKS5 success reply
            match proto.reply_success(relay_address).await {
                Ok(s) => Some((udp_stream, s)),
                Err(e) => {
                    debug!("[{}] Failed to send SOCKS5 reply: {:?}", sid, e);
                    mux.send(FrameType::Close, sid, Vec::new()).await;
                    mux.drop_session(sid).await;
                    return None;
                }
            }
        }
        Err(reason) => {
            warn!("[{}] Remote connect failed: {}", sid, reason);

            let _ = proto.reply_error(&ReplyError::GeneralFailure).await;
            mux.drop_session(sid).await;
            return None;
        }
    }
}
/// Wait for a path to the server, then recall the identity and return sig_pub_bytes.
async fn wait_for_path(node: &RnsNode, dest_hash: &[u8; 16]) -> [u8; 32] {
    ensure_path(node, dest_hash, 30).await;

    // Recall identity (retry until available)
    loop {
        if let Some(sig_pub) = recall_sig_pub(node, dest_hash) {
            info!("Path found, identity recalled");
            return sig_pub;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
