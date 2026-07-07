//! Bidirectional TCP ↔ RNS relay — used by both client and server sessions.

use std::net::{Ipv4Addr, ToSocketAddrs};
use std::os::unix::net::SocketAddr;
use std::str::SplitWhitespace;
use std::sync::Arc;
use std::time::Duration;

use fast_socks5::{new_udp_header, parse_udp_request};
use fast_socks5::util::target_addr::{TargetAddr, ToTargetAddr};
use log::{debug, error, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc};
use udp_stream::UdpStream;

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
            println!("{:?}", frame.frame_type);
            match frame.frame_type {
                FrameType::Data => {
                    println!("{:?}", &frame.payload);
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



/// udp must still have an associated tcp connection to detect when the connection is over. 
/// this does not apply to the server (as in rns server) as it detects that the frame is being closed
/// udp doesn't have a connetcion so the server cannot detect that the remote server the client is connecting
/// to is offline per say.
/// 
/// basically, the client can stop the udp connection either by the reticulum link breaking
/// OR the process using the udp connection stops
/// while the server only stops in the first scenario because the server cannot know if the remote
/// server not responding is part of the protocol.
pub async fn relay_bidirectional_udp(
    sid: u32,
    socket: tokio::net::UdpSocket,
    tcp_stream: Option<tokio::net::TcpStream>,
    mux: MuxHandle,
    mut session_rx: mpsc::UnboundedReceiver<Frame>,
    wrap_packets: bool // on the client side whatever application
    // that is using the socksv5 proxy will add a header for where the udp packet is meant
    // to go, so we don't have to add that in ourselves, however on the server side RNS, when
    // the server receives a packet from a remote destination, it must wrap the udp packet with
    // the original location where that packet came from so the client knows of that information.
    // that's on the UDP -> RNS side, on the other side it's reversed.
) {
    let socket = Arc::new(socket);
    let socket1 = socket.clone();

    let mux_fwd = mux.clone();

    let client_local_port_mutex = Mutex::new(None);
    let client_local_port_1 = Arc::new(client_local_port_mutex);
    let client_local_port_2 = client_local_port_1.clone();
    // the socksv5 protocol never sends the port number that the
    // udp relay server should be expecting from the client, and the port number for the udp
    // connection may be different that the tcp port. So the only way to get the port that the client
    // is expecting is to wait until the client has sent data through udp and record that data
    // this does mean that if the client is only ever receiving data from udp, it will never get
    // the right port number because the relay will never know which port to send it through.
    // I reckon that situation is very rare and that most socksv5 clients will account for that
    // but it's possible this breaks here.
    //
    // I might just be being stupid but I can't find a better way of figuring this out. and the spec
    // is pretty vague so https://www.rfc-editor.org/info/rfc1928/
    //
    // you could probably do better than using a mutex cause it's only updated in one thread
    // and read in another, but I odn't know enough fancy rust stuff to actually do that.


    // UDP -> RNS
    let udp_to_rns = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
             let stuff = socket.recv_from(&mut buf).await;
             println!("certified stuff {:?}", stuff);
             match stuff {
                Ok((0,_)) => {println!("end for some reason"); break},
                Ok((n,addr)) => {
                    println!("sending udp to rns data {:?} {:?}", &buf[..n], addr);
                    println!("sending udp to rns data {:?}", String::from_utf8_lossy(&buf[..n]));
                    println!("sid: {:?} ", sid);
                    if wrap_packets {
                        println!("sending packet raw");
                        let mut a =  client_local_port_1.lock().await;
                        *a = Some(addr.port());

                        mux_fwd.send(FrameType::Data, sid, buf[..n].to_vec());
                    } else {
                        let mut packet = new_udp_header(addr).expect("cannot wrap udp packet");
                        packet.extend_from_slice(&buf[..n]);
                        println!("sending with stuff {:?}", packet);
                        mux_fwd.send(FrameType::Data, sid, packet.to_vec());
                        
                    }
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
        loop {
            if let Some(frame) = session_rx.recv().await {
                println!("killed");
                println!("{:?}", frame);
                if wrap_packets {
                    match frame.frame_type {
                        FrameType::Data => {
                           let a =  client_local_port_2.lock().await;
                           let value = *a;

                           if let Some(port) = value {
                                println!("port: {:?}", port);
                                if let Err(e) = socket1.send_to(&frame.payload, (Ipv4Addr::LOCALHOST,port)).await {
                                    warn!("[{}] UDP write error: {}", sid, e);
                                    break;
                                } else {
                                    println!("sent packet")
                                };
                               
                           } else {
                               warn!("UDP received but client side does not know of a port");
                               // break // shouldn't break because this might not be the client's fault
                               // maybe some random bot send a udp request to that port before the client
                               // could do anything, so we just leave it open.
                           }  
                        }
                        FrameType::Close => {println!("frame closed {:?}", frame); break},
                        _ => {}
                    }
                } else {
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
            } else {
                println!("ended socket stream?");
                break;
                
             };
            
        }
        println!("ended");
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



pub async fn relay_forwarded_tcp(
    sid: u32,
    stream: tokio::net::TcpStream, // stream between local forwarded port and the port
    // of whatever application is connecting to it.
    mux: MuxHandle,
    mut session_rx: mpsc::UnboundedReceiver<Frame>)
{
    let (mut tcp_read, mut tcp_write) = stream.into_split();
    let mux_fwd = mux.clone();

    // TCP -> RNS
    let tcp_to_rns = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match tcp_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    println!("{:?} {:?}", sid, &buf[..n]);
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
                    println!("{:?}", &frame.payload);
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

pub async fn relay_forwarded_udp(
    sid: u32,
    stream: UdpStream, // stream between local forwarded port and the port
    // of whatever application is connecting to it.
    mux: MuxHandle,
    mut session_rx: mpsc::UnboundedReceiver<Frame>,
    server_port: u16)
{
    let mux_fwd = mux.clone();

    let localhost_server_addr = (Ipv4Addr::LOCALHOST, server_port).to_target_addr().unwrap();

    let (mut udp_read,mut udp_write) = tokio::io::split(stream);

    // UDP -> RNS
    let tcp_to_rns = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match udp_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    println!("{:?} {:?}", sid, &buf[..n]);
                    let mut packet = new_udp_header(localhost_server_addr.clone() ).expect("cannot wrap udp packet");
                    packet.extend_from_slice(&buf[..n]);
                    println!("sending with stuff {:?}", packet);
                    mux_fwd.send(FrameType::Data, sid, packet.to_vec());
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
                    println!("{:?}", &frame.payload);
                    if let Err(e) = udp_write.write_all(&frame.payload).await {
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


