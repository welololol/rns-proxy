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
