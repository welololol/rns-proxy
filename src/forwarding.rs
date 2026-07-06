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

use std::sync::Arc;

use log::{error, info, warn};
use tokio::{net::TcpListener, sync::Notify};

use crate::{mux::MuxHandle, relay::relay_forwarded_tcp};


pub struct ForwardedPort {
    pub server_port: u16,
    pub client_port: u16
}


pub async fn udp_tunnel(server_port: u16, client_port: u16) {
    // 1. open up udp listener
    // 2. have a mutex list of all the udps associated with each server and client ports
    // when a new udp port fires to the localhost port, UDP associate to get a corresponding
    // udp port server side.
    // 3. When you get a new udp packet from the socket, find the associated sid and mux
    // from the mutex list.
    // 4. when we receive a packet we should know based on the sid and mux which udp localhost
    // port that is associated with and we can just pass it through.
}

pub async fn tcp_tunnel(mux: MuxHandle, reconnect_notify: Arc<Notify> , port: ForwardedPort) {

// basically:
// 1. open up tcp listener
// 2. every type a program like a webpage tries to connect to the local socket
// it must be from a different tcp port. So we know what program is what based on that.
// so we send a connect request to the reticulm server socks server a CONNECT request
// 3. any time a new tcp packet arrives then add the destination localhost port of the reticulum server
// and any time we receive data we figure out which stream it corresponds to and send it back
    
    let listener = match TcpListener::bind(format!("127.0.0.1:{}",port.client_port)).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to local port at {}", port.client_port);
            return;
        }
    };


    let buf = &mut [0u8 ;65536];
    // Accept SOCKS5 connections
    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, addr) = match accept_result {
                    Ok(sa) => sa,
                    Err(e) => {
                        continue;
                    }
                };
                if !mux.is_connected() {
                    drop(stream);
                    continue;
                }

                let sid = mux.next_session_id();
                let session_rx = mux.register_session(sid);
                let mux_clone = mux.clone();

                println!("{:?} {:?}", sid, addr);

                tokio::spawn(async move {
                    relay_forwarded_tcp(sid, stream, mux_clone, session_rx).await;
                });
            }
            _ = reconnect_notify.notified() => {
                // Link was re-established, just continue accepting
                info!("port {} back", port.client_port);
            }

        }
    }
}
