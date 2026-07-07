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

pub enum ForwardedPortType {
    Tcp,
    Udp,
    TcpUdp, // both
}

pub struct ForwardedPort {
    pub server_port: u16,
    pub client_port: u16,
    pub r#type: ForwardedPortType
}

pub async fn udp_tunnel(mux: MuxHandle, reconnect_notify: Arc<Notify> , port: ForwardedPort) {
    let target_addr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), port.client_port);
    let listener = match UdpListener::bind(target_addr).await {
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
                if !mux.is_connected() {
                    drop(stream);
                    continue;
                }

                let sid = mux.next_session_id();
                let mut session_rx = mux.register_session(sid);
                let mux_clone = mux.clone();

                // let connect_result = udp_bind_connect(sid,mux.clone(), &mut session_rx, target_addr).await;

                let target_addr = TargetAddr::Ip(SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), port.server_port ));

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

/// 1. open up udp listener
/// 2. have a mutex list of all the udps associated with each server and client ports
/// when a new udp port fires to the localhost port, UDP associate to get a corresponding
/// udp port server side.
/// 3. When you get a new udp packet from the socket, find the associated sid and mux
/// from the mutex list.
/// 4. when we receive a packet we should know based on the sid and mux which udp localhost
/// port that is associated with and we can just pass it through.
///
///
/// This implementation is very weird compared to the others cause we can't seperate every udp into
/// different streams and unlike the standard socksv5 implementation we don't have a seperate relay port
/// for every program trying to use udp, so the pattern that works for rest of the functions doesn't work
/// so instead we just handle everything in one function and don't use tokio:spawn which means it's
/// single threaded and a bit more inefficient.
/// 
/// Note, we don't bother with reconnect_notify cause udp is lossy anyways. If it doesn't get through
/// then too bad. better to ignore it than to unbind the udp socket or something.
/// the only other alternative is buffering the udp but I'm not bothering, the application layer
/// can deal with 100% packet loss for like 5 seconds probs.
///
/// Note 2, we have no way of telling the server we are done with udp directly as it's connectionless but we
/// could therotically ask the OS if that localhost udp socket we are receiving from is still open and figure
/// out if we can safely tell the server to delete that port in the lsit. Cause right now they just stay
/// till we disconnect the RNS link or we end the program, so we therotically could run out of
/// udp ports server side. Not really that much of a concern cause we should have a server side
/// limit anyways.
// pub async fn udp_tunnel(mux: MuxHandle, reconnect_notify: Arc<Notify> , port: ForwardedPort) {

//     let listener = match UdpSocket::bind(format!("127.0.0.1:{}",port.client_port)).await {
//         Ok(l) => l,
//         Err(e) => {
//             error!("Failed to local udp port at {}", port.client_port);
//             return;
//         }
//     };


//     let mut port_mapping: Vec<(u32,u16, UnboundedSender<Vec<u8>>)> = Vec::new();
//     // sid, local port number, sender from localhost udp to rns.

//     loop {
//         let mut buf = [0u8 ;65536]; // probably inefficient?
//         let accept_result = listener.recv_from(&mut buf).await; 
//         let (size, addr) = match accept_result {
//             Ok(sa) => sa,
//             Err(e) => {
//                 continue;
//             }
//         };
//         // let data = &(buf[..size]);
//         let associated_port = port_mapping.iter().find(|p| {p.1 == addr.port()} ); 
//          match associated_port {
//             Some((sid, client_port, sender)) => { // either it already exists in the mapping
//                 let mut data = vec![];
//                 data.extend_from_slice(&buf[..size]);
//                 println!("{:?}", sender.send(data));
//             },
//             None => {
//                 // let 
//                 let (sender, receiver) = unbounded_channel();  
//                 //
//                 // put the thing into port_mapping;
//                 port_mapping.push((0,addr.port(), sender));


//                 if !mux.is_connected() {
//                     warn!("No RNS UDP link, rejecting connection");
//                     continue;
//                 }

//                 let sid = mux.next_session_id();
//                 let session_rx = mux.register_session(sid);
//                 let mux_clone = mux.clone();
                
//                 tokio::spawn(async move {
//                     handle_socks5_session(sid, stream, mux_clone, session_rx).await;
//                 });
//             },
//         };
        


                
//                 // let mux = reference.clone();
//                 // if !mux.is_connected() {
//                 //     drop(stream);
//                 //     continue;
//                 // }

//                 // let sid = mux.next_session_id();
//                 // let mut session_rx = mux.register_session(sid);
//                 // let mux_clone = mux.clone();

//                 // println!("{:?} {:?}", sid, addr);
//                 // let target_addr = TargetAddr::Ip(SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), port.server_port ));

//                 // tokio::spawn(async move {
//                 //     if let Ok(_) = connect_tcp_server_side(sid,  mux.clone(), &mut session_rx, target_addr ).await {
//                 //         relay_forwarded_tcp(sid, stream, mux_clone, session_rx).await;
//                 //     }
//                 // });

//     }
//     // note, this implementation doesn't actually do this. We assign one pory server side and that's
//     // it and the implementation only works when a single program is using the udp port. Otherwise
//     // we have to create a new udp socket server side for every single application
// }

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
                if !mux.is_connected() {
                    drop(stream);
                    continue;
                }

                let sid = mux.next_session_id();
                let mut session_rx = mux.register_session(sid);
                let mux_clone = mux.clone();

                println!("{:?} {:?}", sid, addr);
                let target_addr = TargetAddr::Ip(SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), port.server_port ));

                tokio::spawn(async move {
                    if let Ok(_) = connect_tcp_server_side(sid,  mux.clone(), &mut session_rx, target_addr ).await {
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
