use std::time::Duration;

use serde_json::json;

/// Router A: publisher in router mode.
/// Connects to Router B at tcp/127.0.0.1:7448.
/// Listens on tcp/127.0.0.1:7447.
/// Publishes "hello" every second on key "demo/test".
#[tokio::main]
async fn main() {
    zenoh::init_log_from_env_or("error");

    let mut config = zenoh::Config::default();
    config.insert_json5("mode", &json!("router").to_string()).unwrap();
    config.insert_json5("listen/endpoints", &json!(["tcp/127.0.0.1:7447"]).to_string()).unwrap();
    config.insert_json5("connect/endpoints", &json!(["tcp/127.0.0.1:7448"]).to_string()).unwrap();
    // Disable multicast scouting so reconnection relies solely on the connect config
    config.insert_json5("scouting/multicast/enabled", &json!(false).to_string()).unwrap();

    println!("[Router A] Opening session (router mode, connecting to B at :7448)...");
    let session = zenoh::open(config).await.unwrap();

    let publisher = session.declare_publisher("demo/test").await.unwrap();

    println!("[Router A] Publishing on 'demo/test'. Press CTRL-C to quit.");
    for i in 0u64.. {
        let msg = format!("hello #{i}");
        println!("[Router A] Put: '{msg}'");
        publisher.put(&msg).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
