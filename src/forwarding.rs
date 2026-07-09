//! port forwarding command
//! can either be done server side or client side.
//! for server side it just runs a regular socksv5 server but disables all possible addresses except
//! localhost for the specified ports. A regular socksv5 server is backwards compatiable and would work
//!
//! for client side, things are slightly changed, internally a socksv5 proxy is still used
//! but it's not exposed, instead ports whatever ports you selected. is opened and whatever traffic
//! you send to it get forwarded to the remote server to the remote socket. allowing you to directly
//! connect an application to that port and have it be connected to the remote server.
//!
//! rns-proxy open-port -U 43 -u 80 443
//! rns-proxy forward -U 43:43 -u 80:80 443:443
//!
//! rns-proxy open-port/forward by default only forwards/connects to tcp
//! -u flag attempts does both udp and tcp
//! -U flag means udp only
//!
//!
//! 

use std::{net::{Ipv4Addr, SocketAddr}, sync::Arc};

use fast_socks5::util::target_addr::TargetAddr;
use log::{error, info, warn};
use tokio::{net::{TcpListener, UdpSocket}, sync::{Notify, mpsc::{UnboundedReceiver, UnboundedSender, channel, unbounded_channel}}};
use udp_stream::UdpListener;

use crate::{client::{connect_tcp_server_side, udp_bind_connect}, mux::MuxHandle, relay::{relay_forwarded_tcp, relay_forwarded_udp}};

#[derive(Clone, Debug)]
pub enum PortType {
    Tcp,
    Udp,
    TcpUdp, // both
}

#[derive(Clone, Debug)]
pub struct ForwardedPort {
    pub server_port: u16,
    pub client_port: u16,
    pub r#type: PortType
}

/// basically:
/// 1. open up tcp listener
/// 2. every type a program like a webpage tries to connect to the local socket
/// it must be from a different tcp port. So we know what program is what based on that.
/// so we send a connect request to the reticulm server socks server a CONNECT request
/// 3. any time a new tcp packet arrives then add the destination localhost port of the reticulum server
/// and any time we receive data we figure out which stream it corresponds to and send it back
///
///
pub async fn tcp_tunnel(mux: MuxHandle, reconnect_notify: Arc<Notify> , port: ForwardedPort) {
    let listener = match TcpListener::bind(format!("127.0.0.1:{}",port.client_port)).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to local port at {}", port.client_port);
            return;
        }
    };


    let reference = &mux;

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, addr) = match accept_result {
                    Ok(sa) => sa,
                    Err(e) => {
                        continue;
                    }
                };
                let mux = reference.clone();
                if !mux.is_connected().await {
                    drop(stream);
                    continue;
                }

                let sid = mux.next_session_id().await;
                let mut session_rx = mux.register_session(sid).await;
                let mux_clone = mux.clone();

                let target_addr = TargetAddr::Ip(SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), port.server_port)); // ask for any port client server side

                tokio::spawn(async move {
                    if let Some(_) = connect_tcp_server_side(sid,  mux.clone(), &mut session_rx, target_addr ).await {
                        relay_forwarded_tcp(sid, stream, mux_clone, session_rx).await;
                    }
                });
            }
            _ = reconnect_notify.notified() => {
                // Link was re-established, just continue accepting
                info!("port {} back", port.client_port);
            }

        }
    }
}

/// Note 1, we have no way of telling the server we are done with udp directly as it's connectionless but we
/// could therotically ask the OS if that localhost udp socket we are receiving from is still open and figure
/// out if we can safely tell the server to delete that port in the lsit. Cause right now they just stay
/// till we disconnect the RNS link or we end the program, so we therotically could run out of
/// udp ports server side. Not really that much of a concern cause we should have a server side
/// limit anyways.
pub async fn udp_tunnel(mux: MuxHandle, reconnect_notify: Arc<Notify> , port: ForwardedPort) {
    let target_addr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), port.client_port);
    let listener = match UdpListener::bind(target_addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to local port at {}", port.client_port);
            return;
        }
    };

                    info!("d");

    let reference = &mux;

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, addr) = match accept_result {
                    Ok(sa) => sa,
                    Err(e) => {
                        info!("hi");
                        continue;
                    }
                };
                    info!("5");
                let mux = reference.clone();
                info!("1");
                if !mux.is_connected().await {
                    drop(stream);
                    continue;
                }
                info!("2");

                let sid = mux.next_session_id().await;
                info!("2");
                let mut session_rx = mux.register_session(sid).await;
                info!("2");
                let mux_clone = mux.clone();
                info!("2");

                info!("2");
                // let connect_result = udp_bind_connect(sid,mux.clone(), &mut session_rx, target_addr).await;

                let target_addr = TargetAddr::Ip(SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), port.server_port ));

                info!("3");
                tokio::spawn(async move {
                    if let Ok(_) = udp_bind_connect(sid,  mux.clone(), &mut session_rx, target_addr ).await {
                        relay_forwarded_udp(sid, stream, mux_clone, session_rx, port.server_port).await;
                    }
                });
            }
            _ = reconnect_notify.notified() => {
                // Link was re-established, just continue accepting
                info!("port {} back", port.client_port);
            }

        }
    }
}
