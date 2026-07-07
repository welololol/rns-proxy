//! RNS SOCKS5 Proxy Service
//!
//! A SOCKS5 proxy that tunnels TCP connections over the Reticulum Network Stack.
//! Run as either a server (exit node) or client (local SOCKS5 proxy).

use clap::Parser;
use rns_proxy::{cli::{Cli, Commands}, client::run_client_forward, filter::{Filter, FilterConfig, FilterResult, PortFilter}, forwarding::{ForwardedPort, PortType}};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Init logging
    let log_level = if cli.debug { "debug" } else { "info" };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level))
        .format_timestamp_secs()
        .init();

    match cli.command {
        Commands::Server { identity_file } => {
            rns_proxy::server::run_server(identity_file.as_deref(),

                FilterConfig {
                    filters: vec![Filter {
                        address_filter: rns_proxy::filter::AddressFilter::All,
                        port_filter: PortFilter{
                            port_filter: rns_proxy::filter::PortFilterType::All,
                            port_type: PortType::TcpUdp,
                        },
                        filter_result: FilterResult::Include,
                    }],
                }

            ).await;
        }
        Commands::Client {
            destination,
            listen,
        } => {
            rns_proxy::client::run_client(&destination, &listen).await;
        }
        Commands::Connect { destination, ports} => {
            run_client_forward(&destination, vec![ForwardedPort{
                server_port: 34197,
                client_port: 34197,
                r#type: PortType::Udp
                
            }]).await;
            // rns_proxy::server::run_server(identity_file.as_deref()).await;
        }
        Commands::Forward {identity_file, ports} => {
            // run_client_forward(&destination, vec![ForwardedPort{
            //     server_port: 34197,
            //     client_port: 34197,
            //     r#type: ForwardedPortType::Udp
                
            // }]).await;
            // // rns_proxy::server::run_server(identity_file.as_deref()).await;
            rns_proxy::server::run_server(identity_file.as_deref(),
                FilterConfig {
                    filters: vec![Filter {
                        address_filter: rns_proxy::filter::AddressFilter::All,
                        port_filter: PortFilter{
                            port_filter: rns_proxy::filter::PortFilterType::All,
                            port_type: PortType::TcpUdp,
                        },
                        filter_result: FilterResult::Include,
                    }],
                }).await;
                
        }
    }
}
