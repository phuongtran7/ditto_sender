extern crate flexbuffers;

use clap::{App, Arg};
use flexbuffers::{Builder, Reader};
use futures::executor::block_on;
use std::{io, process, thread, time::Duration};

use paho_mqtt as mqtt;

fn main() {
    let matches = App::new("Ditto Client")
        .version("0.1.0")
        .about("Receive and parse Ditto output")
        .arg(
            Arg::new("address")
                .short('a')
                .long("address")
                .about("Sets the MQTT broker address")
                .takes_value(true)
                .default_value("127.0.0.1"),
        )
        .arg(
            Arg::new("topic")
                .short('t')
                .long("topic")
                .about("Sets the topic to subscribe")
                .takes_value(true)
                .default_value("TestTopic"),
        )
        .get_matches();

    let address = matches.value_of("address").unwrap();
    let topic = matches.value_of("topic").unwrap();

    // Create the client.
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(address)
        .client_id("")
        .finalize();

    // Create the client connection
    let mut cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
        println!("Error creating the client: {:?}", e);
        process::exit(1);
    });

    if let Err(err) = block_on(async {
        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .mqtt_version(mqtt::MQTT_VERSION_3_1_1)
            .clean_session(true)
            .finalize();

        // Make the connection to the broker
        println!("Connecting to the MQTT server...");
        cli.connect(conn_opts).await?;

        let mut count = 0.0;
        let mut builder = Builder::default();

        loop {
            let mut map_start = builder.start_map();
            map_start.push(
                "nav_frequency_hz",
                &[count + 1.0, count + 2.0, count + 3.0, count + 4.0],
            );

            map_start.end_map();

            let data = builder.view();

            let msg = mqtt::Message::new(topic, data, mqtt::QOS_1);
            cli.publish(msg).await?;
            count += 1.0;
            builder.reset();
            thread::sleep(Duration::from_millis(1000));
        }

        // Explicit return type for the async block
        Ok::<(), mqtt::Error>(())
    }) {
        eprintln!("{}", err);
    }

    io::stdin().read_line(&mut String::new()).unwrap();

    if cli.is_connected() {
        cli.unsubscribe(topic);
        cli.disconnect(None);
    }
}
