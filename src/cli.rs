use clap::{Parser, Subcommand};
use prse::try_parse;

#[derive(Parser)]
#[command(name = "rns-proxy")]
#[command(about = "SOCKS5 proxy over Reticulum Network Stack")]
#[command(version)]
pub struct Cli {
    /// Enable debug logging
    #[arg(long, global = true)]
    pub debug: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run the SOCKS5 proxy server (exit node)
    Server {
        /// Path to the identity file for persistent server address.
        /// Defaults to ~/.reticulum/rns_proxy_identity
        #[arg(long, value_name = "PATH")]
        identity_file: Option<String>,
    },

    /// Run the SOCKS5 proxy client (local proxy)
    Client {
        /// RNS destination hash (hex)
        #[arg(short, long)]
        destination: String,

        /// Local SOCKS5 listen address
        #[arg(short, long, default_value = "127.0.0.1:1080")]
        listen: String,
    },
    Connect  {
        #[arg(short, long)]
        destination: String,

        // server to client ports.
        #[arg(short, long, value_parser = port_parser)]
        udp_port: Vec<(u16,u16)>,

        #[arg(short, long, value_parser = port_parser)]
        tcp_port: Vec<(u16,u16)>,

        #[arg(short, long, value_parser = port_parser)]
        both_port: Vec<(u16,u16)>, // both udp and tcp
    },
    Forward  {
        #[arg(long, value_name = "PATH")]
        identity_file: Option<String>,

        #[arg(short, long)]
        udp_port: Vec<u16>,

        #[arg(short, long)]
        tcp_port: Vec<u16>,

        #[arg(short, long)]
        both_port: Vec<u16>, // both udp and tcp
    }
}



fn port_parser(s: &str) -> Result<(u16,u16),String> {
    if let Ok((server_port,client_port)) = try_parse!(s,"{}:{}") {
        return Ok((server_port,client_port));
    }
    if let Ok(port) = try_parse!(s,"{}") {
        return Ok((port,port));
    }


    return Err("Invalid port input".into());
}
