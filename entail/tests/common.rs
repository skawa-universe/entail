use std::net::TcpStream;
use std::sync::Once;

static RING_INIT: Once = Once::new();

pub fn init_ring() {
    RING_INIT.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .unwrap();
    });
}

static CHECK_SERVER_INIT: Once = Once::new();

pub fn check_server() {
    CHECK_SERVER_INIT.call_once(|| {
        let deps = [("Datastore", "127.0.0.1:8393")];

        for (name, addr) in deps {
            if TcpStream::connect(addr).is_err() {
                panic!(
                    "\n\x1b[1;31m\
                    ==========================================================\n\
                    [!] INTEGRATION TEST ERROR: {} NOT FOUND\n\
                    ==========================================================\n\
                    Target Address: {}\n\n\
                    The integration tests cannot proceed without this service.\n\
                    Please ensure your environment is running:\n\n\
                    $ docker-compose up -d\n\
                    ==========================================================\n\
                    \x1b[0m",
                    name.to_uppercase(),
                    addr
                );
            }
        }
    });
}
