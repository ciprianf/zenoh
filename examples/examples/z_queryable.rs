//
// Copyright (c) 2023 ZettaScale Technology
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

    println!("Declaring Queryable on 'health-status'...");
    let queryable = session
        .declare_queryable("health-status")
        .complete(true)
        .await
        .unwrap();

    // Build a 1MB reply payload
    let reply_payload = vec![b'R'; PAYLOAD_SIZE];

    println!("Press CTRL-C to quit...");
    while let Ok(query) = queryable.recv_async().await {
        let query_len = query.payload().map_or(0, |p| p.len());
        println!(
            ">> [Queryable] Received Query '{}' ({} bytes)",
            query.selector(),
            query_len,
        );
        println!(">> [Queryable] Responding with 1MB payload...");
        query
            .reply("health-status", reply_payload.clone())
            .await
            .unwrap_or_else(|e| println!(">> [Queryable] Error sending reply: {e}"));
    }
}

#[derive(Parser, Clone, Debug)]
struct Args {
    #[command(flatten)]
    common: CommonArgs,
}
