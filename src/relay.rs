//! Bidirectional TCP ↔ RNS relay — used by both client and server sessions.

use std::sync::Arc;
use std::time::Duration;

use fast_socks5::parse_udp_request;
use fast_socks5::util::target_addr::{TargetAddr, ToTargetAddr};
use log::{debug, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::SocketAddr;
use tokio::sync::mpsc;

use crate::frame::{Frame, FrameType};
use crate::mux::MuxHandle;

/// Relay data bidirectionally between a TCP stream and an RNS mux session.
///
/// Sends a `Close` frame and drops the session when either direction finishes.
pub async fn relay_bidirectional_tcp(
    sid: u32,
    stream: tokio::net::TcpStream,
    mux: MuxHandle,
    mut session_rx: mpsc::UnboundedReceiver<Frame>,
) {
    let (mut tcp_read, mut tcp_write) = stream.into_split();
    let mux_fwd = mux.clone();

    // TCP -> RNS
    let tcp_to_rns = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match tcp_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    mux_fwd.send(FrameType::Data, sid, buf[..n].to_vec());
                }
                Err(e) => {
                    debug!("[{}] TCP read error: {}", sid, e);
                    break;
                }
            }
        }
    });

    // RNS -> TCP
    let rns_to_tcp = tokio::spawn(async move {
        while let Some(frame) = session_rx.recv().await {
            match frame.frame_type {
                FrameType::Data => {
                    if let Err(e) = tcp_write.write_all(&frame.payload).await {
                        warn!("[{}] TCP write error: {}", sid, e);
                        break;
                    }
                }
                FrameType::Close => break,
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = tcp_to_rns => {},
        _ = rns_to_tcp => {},
    }

    mux.send(FrameType::Close, sid, Vec::new());
    mux.drop_session(sid);
}



// udp must still have an associated tcp connection to detect when the connection is over. 
// this does not apply to the server (as in rns server) as it detects that the frame is being closed
// udp doesn't have a connetcion so the server cannot detect that the remote server the client is connecting
// to is offline per say.
// 
// basically, the client can stop the udp connection either by the reticulum link breaking
// OR the process using the udp connection stops
// while the server only stops in the first scenario because the server cannot know if the remote
// server not responding is part of the protocol.
pub async fn relay_bidirectional_udp(
    sid: u32,
    socket: tokio::net::UdpSocket,
    tcp_stream: Option<tokio::net::TcpStream>,
    mux: MuxHandle,
    mut session_rx: mpsc::UnboundedReceiver<Frame>,
) {
    let socket = Arc::new(socket);
    let socket1 = socket.clone();

    let mux_fwd = mux.clone();


    // UDP -> RNS
    let udp_to_rns = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match socket.recv(&mut buf).await {
                Ok(0) => {println!("end for some reason"); break},
                Ok(n) => {
                    println!("sending udp to rns data {:?}", &buf[..n]);
                    println!("sending udp to rns data {:?}", String::from_utf8_lossy(&buf[..n]));
                    println!("sid: {:?} ", sid);
                    mux_fwd.send(FrameType::Data, sid, buf[..n].to_vec());
                }
                Err(e) => {
                    debug!("[{}] UDP read error: {}", sid, e);
                    break;
                }
            }
        }
    });

    // RNS -> UDP
    let rns_to_udp = tokio::spawn(async move {
        while let Some(frame) = session_rx.recv().await {
            match frame.frame_type {
                FrameType::Data => {
                    match parse_udp_request(&*frame.payload).await {
                        Ok((frag,addr,data)) => {
                            println!("sending rns to udp data {:?}:{:?}:{:?}", frag, addr, data);
                            println!("sending rns to udp data {:?}", String::from_utf8_lossy(data));
                            println!("sending from: {:?} to {:?}", socket1.local_addr(), addr);

                            let target  = addr.into_string_and_port(); // string conversion is the only way to convert
                            println!("{:?}", target);
                            // between the tokio and fastsocksv5 versions for some reason
                            
                            if let Err(e) = socket1.send_to(data,target).await {
                                warn!("[{}] UDP write error: {}", sid, e);
                                break;
                            } else {
                                println!("sent packet")
                            }
                        }
                        Err(e) => {
                            debug!("[{}] UDP read error: {}", sid, e);
                            break

                        }
                        
                    };

                }
                FrameType::Close => {println!("frame closed {:?}", frame); break},
                _ => {}
            }
        }
    });


    let break_connection_tcp_check = match tcp_stream {
        Some(tcp_stream) =>  {
            let (mut tcp_read, mut tcp_write) = tcp_stream.into_split();

            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                loop {
                    match tcp_read.read(&mut buf).await {
                        Ok(0) => {
                            debug!("tcp connectioned associated with udp died {:?}", sid);
                            break;
                        },
                        Ok(n) => {
                            warn!("client still sending tcp through udp port {:?} {:?}", sid, &buf[0..n])
                        }
                        Err(e) => {
                            debug!("[{}] TCP read error: {}", sid, e);
                            break;
                        }
                    }
                }
            })
        }      
        None => {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_hours(100000000000)).await; // lmao // I can't be bothered importing the empty future thing.
            })
        }
    };
    tokio::select! {
        _ = udp_to_rns => {println!("udp end")},
        _ = rns_to_udp => {println!("rns end")},
        _ = break_connection_tcp_check => {println!("tcp end")},
    }

    mux.send(FrameType::Close, sid, Vec::new());
    mux.drop_session(sid);
}
