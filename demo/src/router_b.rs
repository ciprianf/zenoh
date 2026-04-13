use serde_json::json;

/// Router B: subscriber in router mode.
/// Listens on tcp/127.0.0.1:7448.
/// Does NOT connect to Router A (one-sided connect).
/// Subscribes to "demo/test".
#[tokio::main]
async fn main() {
    zenoh::init_log_from_env_or("error");

    let mut config = zenoh::Config::default();
    config.insert_json5("mode", &json!("router").to_string()).unwrap();
    config.insert_json5("listen/endpoints", &json!(["tcp/127.0.0.1:7448"]).to_string()).unwrap();
    // No connect endpoints — only A connects to B
    // Disable multicast scouting so reconnection relies solely on the connect config
    config.insert_json5("scouting/multicast/enabled", &json!(false).to_string()).unwrap();

    println!("[Router B] Opening session (router mode, listening on :7448)...");
    let session = zenoh::open(config).await.unwrap();

    let subscriber = session.declare_subscriber("demo/test").await.unwrap();

    println!("[Router B] Subscribed to 'demo/test'. Press CTRL-C to quit.");
    while let Ok(sample) = subscriber.recv_async().await {
        let payload = sample
            .payload()
            .try_to_string()
            .unwrap_or_else(|e| e.to_string().into());
        println!("[Router B] Received: '{payload}'");
    }
}
