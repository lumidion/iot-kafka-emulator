use rskafka::client::partition::PartitionClient;
use rskafka::{
    client::{error::Error as KafkaClientError, partition::Compression, Client, ClientBuilder},
    record::Record,
    time,
};
use std::str::FromStr;
use std::sync::Arc;
use std::{
    fmt, fs, thread, time as std_time,
    time::{SystemTime, UNIX_EPOCH},
};

use rand::Rng;

use log::info;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use time::OffsetDateTime;
use tokio;
use uuid::Uuid;

use async_stream::stream;
use futures::{pin_mut, StreamExt};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Duration;

type Result<T> = std::result::Result<T, ApplicationError>;

#[derive(Debug, Clone)]
struct ApplicationError {
    msg: String,
}

impl fmt::Display for ApplicationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Application Error")
    }
}

impl ApplicationError {
    fn new(msg: &str) -> ApplicationError {
        Self {
            msg: msg.to_string(),
        }
    }
}

#[derive(Debug)]
enum DataSource {
    Generate,
    Jsonl,
}

impl DataSource {
    fn as_str(&self) -> &'static str {
        match self {
            DataSource::Generate => "generate",
            DataSource::Jsonl => "jsonl",
        }
    }

    fn parse_from_config() -> Result<DataSource> {
        let result_str = match std::env::var("DATA_SOURCE") {
            Ok(value) => value.to_string(),
            Err(_) => "generate".to_string(),
        };

        match result_str.as_str() {
            "generate" => Ok(DataSource::Generate),
            "jsonl" => Ok(DataSource::Jsonl),
            _ => Err(ApplicationError::new(
                format!(
                    "Could not parse {} to a valid data source. Valid options: generate, jsonl",
                    &result_str
                )
                .as_str(),
            )),
        }
    }
}

#[derive(Clone, Debug)]
struct JsonLFile {
    file_path: String,
    contents: Vec<String>,
}
impl JsonLFile {
    fn new(file_path: &str, contents: Vec<String>) -> Self {
        Self {
            file_path: file_path.to_string(),
            contents: contents.to_vec(),
        }
    }
}

#[derive(Debug)]
struct Configuration {
    topic_name: String,
    broker_url: String,
    data_source: DataSource,
    batch_size: u16,
    batch_interval_ms: u64,
    number_of_threads: u16,
}

impl Configuration {
    fn from_env() -> Self {
        let topic_name = get_str_from_env("KAFKA_TOPIC_NAME");
        let broker_url = get_str_from_env("KAFKA_BROKER_URL");
        let data_source = DataSource::parse_from_config().unwrap();

        let batch_size = get_u16_from_env("KAFKA_BATCH_SIZE", Some(20000), 1000);
        let batch_interval_ms = get_u64_from_env("KAFKA_BATCH_INTERVAL", None, 1000);
        let number_of_threads = get_u16_from_env("THREAD_NUMBER", Some(1000), 500);

        Self {
            topic_name,
            broker_url,
            data_source,
            batch_size,
            batch_interval_ms,
            number_of_threads,
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum SensorStatus {
    ON,
    ERROR,
    RESTARTING,
}

impl SensorStatus {
    fn values() -> Vec<SensorStatus> {
        vec![
            SensorStatus::ON,
            SensorStatus::ERROR,
            SensorStatus::RESTARTING,
        ]
    }

    fn random() -> SensorStatus {
        let statuses = SensorStatus::values();
        let mut rng_generator = rand::thread_rng();
        let random_index = rng_generator.gen_range(0..statuses.len());
        statuses[random_index]
    }
}

impl fmt::Display for SensorStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

struct IotRecord<'a> {
    key: &'a KafkaKey,
    timestamp: u128,
    status: SensorStatus,
}

impl<'a> IotRecord<'a> {
    fn for_key(key: &'a KafkaKey) -> Self {
        Self {
            key,
            timestamp: get_current_timestamp_in_ms(),
            status: SensorStatus::random(),
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
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("IotRecord", 1)?;
        state.serialize_field("timestamp", &self.timestamp.to_string().as_str())?;
        state.serialize_field("status", &self.status.to_string())?;
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

fn get_current_timestamp_in_ms() -> u128 {
    let start = SystemTime::now();
    start.duration_since(UNIX_EPOCH).unwrap().as_millis()
}

fn get_opt_str_from_env(key: &str, default_value: &str) -> String {
    match std::env::var(key) {
        Ok(value) => value,
        Err(_) => default_value.to_string(),
    }
}

fn get_str_from_env(key: &str) -> String {
    std::env::var(key).expect(format!("{} should be present in the environment", key).as_str())
}

fn get_u16_from_env(key: &str, upper_limit_option: Option<u16>, default: u16) -> u16 {
    get_num_from_env(key, upper_limit_option, default, u16::MAX)
}

fn get_u64_from_env(key: &str, upper_limit_option: Option<u64>, default: u64) -> u64 {
    get_num_from_env(key, upper_limit_option, default, u64::MAX)
}

fn get_num_from_env<N>(
    key: &str,
    upper_limit_option: Option<N>,
    default: N,
    system_upper_limit: N,
) -> N
where
    N: FromStr + PartialOrd + ToString,
    <N as FromStr>::Err: fmt::Debug,
{
    let value_res = std::env::var(key);

    match value_res {
        Ok(value) => {
            let number_value = value.parse::<N>().expect(
                format!(
                    "{} could not be converted to an integer between 0 and {} for key, {}",
                    value,
                    system_upper_limit.to_string(),
                    key
                )
                .as_str(),
            );

            let final_res = match upper_limit_option {
                Some(upper_limit) => {
                    if number_value <= upper_limit {
                        Ok(number_value)
                    } else {
                        Err(format!(
                            "Maximum value for key {} is set to {}. Value received: {}",
                            key,
                            upper_limit.to_string(),
                            value,
                        ))
                    }
                }
                None => Ok(number_value),
            };
            final_res.unwrap()
        }
        Err(_) => default,
    }
}

async fn produce_records_for_keys<'a>(client: &'a PartitionClient, keys: &'a Vec<KafkaKey>) {
    let records_res: std::result::Result<Vec<Record>, serde_json::Error> = keys
        .into_iter()
        .map(|kafka_key| IotRecord::for_key(&kafka_key).to_kafka_record())
        .collect();

    let records = records_res.expect("Keys should be parseable into kafka records");

    client.produce(records, Compression::Gzip).await.unwrap(); //TODO: compression here should probably be specified in config
}

async fn continuously_produce_records_for_keys(
    client: &PartitionClient,
    batch_size: &u16,
    batch_interval_ms: &u64,
) {
    let keys = generate_keys(&batch_size);
    loop {
        produce_records_for_keys(&client, &keys).await;
        thread::sleep(std_time::Duration::from_millis(batch_interval_ms.clone()));
    }
}

fn generate_keys(number_of_keys: &u16) -> Vec<KafkaKey> {
    Vec::from_iter(0..number_of_keys.clone())
        .into_iter()
        .map(|_| KafkaKey::new())
        .collect()
}

async fn create_topic(client: &Client, topic_name: &str) {
    info!("Creating topic");
    let controller_client = client.controller_client().unwrap();
    let res = controller_client
        .create_topic(topic_name, 2, 1, 5_000)
        .await;

    match res {
        Ok(_) => info!("Topic created"),
        Err(KafkaClientError::ServerError(err, _)) => {
            let msg = format!("{}", err);
            if msg == "TopicAlreadyExists" {
                info!("Topic already exists. Continuing to message production.")
            } else {
                panic!("{}", err)
            }
        }
        Err(err) => panic!("{}", err),
    }
}

fn get_jsonl_file_paths_from_dir(dir_path: &str) -> Vec<String> {
    let mut subdir_files: Vec<String> = Vec::new();
    let mut file_paths = fs::read_dir(dir_path)
        .expect(format!("Directory not found: {}", dir_path).as_str())
        .into_iter()
        .flat_map(|entry_res| match entry_res {
            Ok(entry) => {
                let path = entry.path();
                if path.is_file() {
                    path.extension().map(|ext| match ext.to_str() {
                        Some("jsonl") => entry.path().to_str().map(|res| res.to_string()),
                        _ => None,
                    })
                } else {
                    let mut files_in_subdir = get_jsonl_file_paths_from_dir(path.to_str().unwrap()); //TODO: could fail if user passes a subdir or filename with non-Unicode chars
                    subdir_files.append(&mut files_in_subdir);
                    None
                }
            }
            _ => None,
        })
        .flatten()
        .collect::<Vec<String>>();

    file_paths.append(&mut subdir_files);
    file_paths
}

fn read_jsonl_from_file(path: &str) -> JsonLFile {
    let file = File::open(path).expect(format!("Could not open file for path: {}", path).as_str());
    let reader = BufReader::new(file);
    let mut contents: Vec<String> = Vec::new();

    for (index, line) in reader.lines().into_iter().enumerate() {
        let line = line.expect(
            format!(
                "Unable to read line {} for file path: {}",
                index.to_string().as_str(),
                path
            )
            .as_str(),
        );
        contents.push(line);
    }

    JsonLFile::new(path, contents)
}

async fn send_json_file_to_kafka<'a>(
    client: &'a PartitionClient,
    file: &JsonLFile,
    batch_size: &u16,
) {
    let cloned_contents = file.clone().contents;
    let chunks: Vec<Vec<String>> = cloned_contents
        .chunks(usize::from(batch_size.to_owned()))
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<Vec<String>>>();

    let producer_stream = stream! {
        for json_chunk in chunks {
            let records = json_chunk
                .into_iter()
                .map(|chunk| Record {
                    key: None,
                    value: Some(chunk.as_bytes().to_vec()),
                    headers: Default::default(),
                    timestamp: OffsetDateTime::now_utc(),
                })
                .collect();
            yield client.produce(records, Compression::Gzip).await.unwrap();
        }
    };

    pin_mut!(producer_stream);

    while let Some(value) = producer_stream.next().await {}
}

async fn run() {
    let configuration = Configuration::from_env();
    info!("Successfully loaded configuration: {:?}", configuration);

    let connection = configuration.broker_url.to_owned();
    let client = ClientBuilder::new(vec![connection]).build().await.unwrap();

    create_topic(&client, configuration.topic_name.as_str()).await;

    let partition_client: Arc<PartitionClient> = Arc::new(
        client
            .partition_client(configuration.topic_name.to_owned(), 0)
            .unwrap(),
    );
    match configuration.data_source {
        DataSource::Generate => {
            for _ in 0..configuration.number_of_threads {
                let cloned_client = partition_client.clone();
                let cloned_batch_size = Arc::new(configuration.batch_size);
                let cloned_batch_interval_ms = Arc::new(configuration.batch_interval_ms);
                tokio::spawn(async move {
                    continuously_produce_records_for_keys(
                        cloned_client.as_ref(),
                        cloned_batch_size.as_ref(),
                        cloned_batch_interval_ms.as_ref(),
                    )
                    .await
                });
            }
        }
        DataSource::Jsonl => {
            let json_file_paths = get_jsonl_file_paths_from_dir("/app/data/jsonl");
            let jsonl_loaded_files: Vec<JsonLFile> = json_file_paths
                .into_iter()
                .map::<JsonLFile, _>(|path| read_jsonl_from_file(path.as_str()))
                .collect::<Vec<JsonLFile>>();

            let file_chunks: Vec<Vec<JsonLFile>> = jsonl_loaded_files
                .chunks(usize::from(configuration.number_of_threads.to_owned()))
                .map(|chunk| chunk.to_vec())
                .collect::<Vec<Vec<JsonLFile>>>();

            for chunk in file_chunks {
                for file in chunk {
                    let cloned_client = partition_client.clone();
                    let cloned_batch_size = Arc::new(configuration.batch_size);
                    let cloned_file = Arc::new(file);

                    tokio::spawn(async move {
                        send_json_file_to_kafka(
                            cloned_client.as_ref(),
                            cloned_file.as_ref(),
                            cloned_batch_size.as_ref(),
                        )
                        .await
                    });
                }
            }
        }
    }

    loop {
        thread::sleep(Duration::from_millis(1000));
    }
}

fn main() {
    env_logger::init();
    info!("Initializing application");

    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(run());
}
