use std::sync::OnceLock;

static RING_INIT: OnceLock<()> = OnceLock::new();

pub fn init_ring() {
    RING_INIT.get_or_init(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .unwrap();
    });
}
