//
// Copyright (c) 2024 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use std::time::Duration;

use clap::Parser;
use zenoh::Config;
use zenoh_examples::CommonArgs;

const PAYLOAD_SIZE: usize = 1_024 * 1_024; // 1MB

#[tokio::main]
async fn main() {
    zenoh::init_log_from_env_or("error");
    let args = Args::parse();
    let mut config: Config = args.common.into();
    config
        .insert_json5("transport/shared_memory/enabled", "true")
        .unwrap();

    println!("Opening session...");
    let session = zenoh::open(config).await.unwrap();

    println!("Declaring Querier on 'health-status'...");
    let querier = session
        .declare_querier("health-status")
        .timeout(Duration::from_secs(10))
        .await
        .unwrap();

    // Build a 1MB query payload
    let query_payload = vec![b'Q'; PAYLOAD_SIZE];

    println!("Press CTRL-C to quit...");
    for idx in 0..u32::MAX {
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!("[{idx:4}] Querying 'health-status' with 1MB payload...");
        let replies = querier
            .get()
            .payload(query_payload.clone())
            .await
            .unwrap();
        while let Ok(reply) = replies.recv_async().await {
            match reply.result() {
                Ok(sample) => {
                    let len = sample.payload().len();
                    println!(
                        ">> Received ('{}': {} bytes)",
                        sample.key_expr().as_str(),
                        len,
                    );
                }
                Err(err) => {
                    let payload = err
                        .payload()
                        .try_to_string()
                        .unwrap_or_else(|e| e.to_string().into());
                    println!(">> Received (ERROR: '{payload}')");
                }
            }
        }
    }
}

#[derive(Parser, Clone, Debug)]
struct Args {
    #[command(flatten)]
    common: CommonArgs,
}
