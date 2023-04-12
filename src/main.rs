use rskafka::client::partition::PartitionClient;
use rskafka::{
    client::{partition::Compression, Client, ClientBuilder},
    record::Record,
    time,
};
use std::error::Error;
use std::io;
use std::io::{Cursor, Read};
use std::sync::{Arc, Mutex};
use std::{thread, time as std_time};

use futures::future::{BoxFuture, FutureExt};
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use time::OffsetDateTime;
use tokio;
use tokio::task::JoinHandle;
use uuid::Uuid;

struct IotRecord<'a> {
    key: &'a KafkaKey,
    timestamp: OffsetDateTime,
}

impl<'a> IotRecord<'a> {
    fn for_key(key: &'a KafkaKey) -> Self {
        Self {
            key,
            timestamp: OffsetDateTime::now_utc(),
        }
    }

    fn to_kafka_record(self: &Self) -> serde_json::Result<Record> {
        let serialization_res = serde_json::to_string(&self);

        serialization_res.map(|value| Record {
            key: Some(self.key.serialize()),
            value: Some(value.as_bytes().to_vec()),
            headers: Default::default(),
            timestamp: OffsetDateTime::now_utc(),
        })
    }
}

impl Serialize for IotRecord<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("IotRecord", 1)?;
        state.serialize_field("timestamp", &self.timestamp.unix_timestamp())?;
        state.end()
    }
}

#[derive(Clone)]
struct KafkaKey {
    value: Uuid,
}

impl KafkaKey {
    fn serialize(self: &Self) -> Vec<u8> {
        self.value.to_string().as_bytes().to_vec()
    }

    fn new() -> Self {
        Self {
            value: Uuid::new_v4(),
        }
    }
}

async fn produce_records_for_keys<'a>(client: &'a PartitionClient, keys: &'a Vec<KafkaKey>) {
    let records_res: Result<Vec<Record>, serde_json::Error> = keys
        .into_iter()
        .map(|kafka_key| IotRecord::for_key(&kafka_key).to_kafka_record())
        .collect();

    let records = records_res.expect("Keys should be parseable into kafka records");

    client
        .produce(records, Compression::default())
        .await
        .unwrap();

    thread::sleep(std_time::Duration::from_millis(500));
}

async fn continuously_produce_records_for_keys(client: &PartitionClient) {
    let keys = generate_keys();
    loop {
        produce_records_for_keys(&client, &keys).await;
        produce_records_for_keys(&client, &keys).await;
        produce_records_for_keys(&client, &keys).await;
        produce_records_for_keys(&client, &keys).await;
        produce_records_for_keys(&client, &keys).await;
        produce_records_for_keys(&client, &keys).await;
        produce_records_for_keys(&client, &keys).await;
        produce_records_for_keys(&client, &keys).await;
        produce_records_for_keys(&client, &keys).await;
        produce_records_for_keys(&client, &keys).await;
    }
}

fn generate_keys() -> Vec<KafkaKey> {
    vec![
        KafkaKey::new(),
        KafkaKey::new(),
        KafkaKey::new(),
        KafkaKey::new(),
        KafkaKey::new(),
        KafkaKey::new(),
        KafkaKey::new(),
        KafkaKey::new(),
        KafkaKey::new(),
        KafkaKey::new(),
    ]
}

async fn create_topic(client: &Client, topic_name: &str) {
    println!("Creating topic");
    let controller_client = client.controller_client().unwrap();
    controller_client
        .create_topic(topic_name, 2, 1, 5_000)
        .await
        .unwrap();

    println!("Topic created");
}

async fn run() {
    let connection = "kafka:9092".to_owned();
    let client = ClientBuilder::new(vec![connection]).build().await.unwrap();

    let topic_name = "sample_topic";

    create_topic(&client, &topic_name).await;

    let partition_client: Arc<PartitionClient> =
        Arc::new(client.partition_client(topic_name.to_owned(), 0).unwrap());

    loop {
        let cloned_client = partition_client.clone();
        tokio::spawn(
            async move { continuously_produce_records_for_keys(cloned_client.as_ref()).await },
        );
    }
}

fn main() {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(run());
}
