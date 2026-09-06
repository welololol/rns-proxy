//! LMXF and `rns://` address parsing.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use fast_socks5::util::target_addr::TargetAddr;

/// A route selected by an LMXF hostname.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LmxfRoute {
    pub destination: [u8; 16],
    pub target: TargetAddr,
}

/// Parse `<target>.<32-hex-destination>.lmxf`.
///
/// A bare `<destination>.lmxf` targets localhost on the remote node. This is
/// useful for services exposed directly by an exit node, e.g. Minecraft.
pub fn parse_lmxf_host(host: &str, port: u16) -> Option<LmxfRoute> {
    let labels: Vec<_> = host.trim_end_matches('.').split('.').collect();
    if labels.len() < 2 || !labels.last()?.eq_ignore_ascii_case("lmxf") {
        return None;
    }

    let destination_label = labels[labels.len() - 2];
    let bytes = hex::decode(destination_label).ok()?;
    if bytes.len() != 16 {
        return None;
    }
    let mut destination = [0u8; 16];
    destination.copy_from_slice(&bytes);

    let target_host = if labels.len() == 2 {
        "127.0.0.1".to_string()
    } else {
        labels[..labels.len() - 2].join(".")
    };

    let target = if let Ok(ip) = target_host.parse::<IpAddr>() {
        TargetAddr::Ip(SocketAddr::new(ip, port))
    } else {
        TargetAddr::Domain(target_host, port)
    };

    Some(LmxfRoute {
        destination,
        target,
    })
}

/// Parse `rns://<destination>/<host>:<port>` for bookmarks and integrations.
pub fn parse_rns_uri(uri: &str) -> Option<LmxfRoute> {
    let rest = uri.strip_prefix("rns://")?;
    let (destination, service) = rest.split_once('/')?;
    let (host, port) = service.rsplit_once(':')?;
    let port = port.parse().ok()?;
    parse_lmxf_host(&format!("{}.lmxf", destination), port).map(|mut route| {
        route.target = if host.is_empty() {
            TargetAddr::Ip(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
        } else if let Ok(ip) = host.parse::<IpAddr>() {
            TargetAddr::Ip(SocketAddr::new(ip, port))
        } else {
            TargetAddr::Domain(host.to_string(), port)
        };
        route
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn parses_bare_service() {
        let route = parse_lmxf_host(&format!("{}.lmxf", HASH), 25565).unwrap();
        assert_eq!(
            route.target,
            TargetAddr::Ip("127.0.0.1:25565".parse().unwrap())
        );
    }

    #[test]
    fn parses_named_service() {
        let route = parse_lmxf_host(&format!("minecraft.{}.lmxf", HASH), 25565).unwrap();
        assert_eq!(route.target, TargetAddr::Domain("minecraft".into(), 25565));
    }

    #[test]
    fn parses_rns_uri() {
        let route = parse_rns_uri(&format!("rns://{}/localhost:25565", HASH)).unwrap();
        assert_eq!(route.target, TargetAddr::Domain("localhost".into(), 25565));
    }
}
