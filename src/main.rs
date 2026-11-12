use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs},
    sync::Arc,
};

use rand::RngExt;
use tokio::net::UdpSocket;

fn get_mac_address() -> String {
    let mut rng = rand::rng();
    let mut mac_address = String::with_capacity(17);
    for _ in 0..6 {
        if !mac_address.is_empty() {
            mac_address.push('-');
        }
        mac_address.push_str(&format!("{:02X}", rng.random::<u8>()));
    }
    mac_address
}

async fn local_ip_for(peer: SocketAddr) -> Option<IpAddr> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await.ok()?;
    socket.connect(peer).await.ok()?;
    Some(socket.local_addr().ok()?.ip())
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args();
    let _ = args.next();

    let sonar_address = args
        .next()
        .expect("missing <IP:PORT> arg for sonar")
        .to_socket_addrs()
        .expect("failed to parse <IP:PORT> arg")
        .next()
        .expect("missing <IP:PORT> arg");
    let IpAddr::V4(sonar_ipv4_address) = sonar_address.ip() else {
        panic!("invalid IPv4 for address '{}'", sonar_address.ip());
    };
    let sonar_bind_address = netdev::get_interfaces()
        .into_iter()
        .flat_map(|interface| interface.ipv4)
        .filter(|address| address.contains(&sonar_ipv4_address))
        .map(|address| address.addr())
        .next()
        .expect("unreachable network for provided address");
    let sonar_udp = Arc::new(
        UdpSocket::bind((sonar_bind_address, 0))
            .await
            .expect("failed to open UDP socket for sonar"),
    );
    sonar_udp
        .connect(sonar_address)
        .await
        .expect("failed to connect to sonar via UDP");

    let port: u16 = match args.next() {
        Some(port_str) => port_str.parse().expect("invalid port"),
        None => 12345,
    };

    let mac_address: &'static str = get_mac_address().leak();
    println!("Using fake MAC address: {mac_address}");

    tokio::spawn(async move {
        println!("Discovery: Listening on 0.0.0.0 port 30303...");
        let discovery_udp = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 30303))
            .await
            .expect("discovery bind port should be free");
        discovery_udp
            .set_broadcast(true)
            .expect("failed to enable broadcast on discovery socket");
        let mut buf = [0u8; 1024];
        loop {
            let Ok((len, addr)) = discovery_udp.recv_from(&mut buf).await else {
                continue;
            };
            if buf[..len].starts_with(b"SetSS1IP") {
                println!(
                    "SetSS1IP: Broadcast received from {addr}: {}",
                    String::from_utf8_lossy(&buf[..len])
                );
                continue;
            } else if &buf[..len] != b"Discovery" {
                continue;
            }
            println!("Discovery: Broadcast received from {addr}");
            let Some(ip) = local_ip_for(addr).await else {
                continue;
            };
            let _ = discovery_udp
                .send_to(
                    format!(
                        "SONAR PING360\r\n\
                        Blue Robotics\r\n\
                        MAC Address:- {mac_address}\r\n\
                        IP Address:- {ip}\r\n\
                        Port:- {port}\r\n"
                    )
                    .as_bytes(),
                    addr,
                )
                .await;
        }
    });

    let client = Arc::new(tokio::sync::Mutex::new(
        None::<(SocketAddr, Arc<UdpSocket>)>,
    ));

    for address in netdev::get_interfaces()
        .into_iter()
        .flat_map(|interface| interface.ipv4)
    {
        let sonar_udp = Arc::clone(&sonar_udp);
        let client = Arc::clone(&client);
        tokio::spawn(async move {
            let ip = address.addr();
            println!("Proxy: Listening on {ip} port {port}...");
            let proxy_udp = Arc::new(
                UdpSocket::bind((ip, port))
                    .await
                    .expect("proxy bind port should be free"),
            );
            let mut recv_buf = [0u8; u16::MAX as usize];
            loop {
                let Ok((len, addr)) = proxy_udp.recv_from(&mut recv_buf).await else {
                    continue;
                };
                let mut client = client.lock().await;
                if len > 0 && client.as_ref().is_none_or(|(c, _)| *c != addr) {
                    println!("Proxy: Request received from {addr}");
                    *client = Some((addr, Arc::clone(&proxy_udp)));
                }
                drop(client);
                if len > 0 {
                    let _ = sonar_udp.send(&recv_buf[..len]).await;
                }
            }
        });
    }

    let mut send_buf = [0u8; u16::MAX as usize];
    loop {
        let Ok(len) = sonar_udp.recv(&mut send_buf).await else {
            continue;
        };
        let client = client.lock().await.clone();
        if let Some((addr, proxy_udp)) = client {
            let _ = proxy_udp.send_to(&send_buf[..len], addr).await;
        }
    }
}
