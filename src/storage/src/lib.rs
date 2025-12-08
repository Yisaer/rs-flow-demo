use std::fs;
use std::path::{Path, PathBuf};

use redb::{Database, ReadableTable, TableDefinition};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const CURRENT_VERSION: u8 = 1;
const STREAMS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("streams");
const PIPELINES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("pipelines");
const SHARED_MQTT_CONFIGS_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("shared_mqtt_client_configs");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageNamespace {
    Metadata,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(String),
    #[error("backend error: {0}")]
    Backend(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("unsupported record version: {0}")]
    UnsupportedVersion(u8),
    #[error("corrupted record: {0}")]
    Corrupted(&'static str),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
}

impl StorageError {
    fn io(err: std::io::Error) -> Self {
        StorageError::Io(err.to_string())
    }

    fn backend(err: impl std::fmt::Display) -> Self {
        StorageError::Backend(err.to_string())
    }

    fn serialization(err: impl std::fmt::Display) -> Self {
        StorageError::Serialization(err.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredStream {
    pub id: String,
    pub stream_type: StoredStreamType,
    pub schema_json: String,
    pub props: StoredStreamProps,
    pub decoder_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StoredStreamType {
    Mqtt,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StoredStreamProps {
    Mqtt(StoredMqttStreamProps),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredMqttStreamProps {
    pub broker_url: String,
    pub topic: String,
    pub qos: u8,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredPipeline {
    pub id: String,
    pub sql: String,
    pub sinks: Vec<StoredSink>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredSink {
    pub id: String,
    pub kind: String,
    pub config_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredMqttClientConfig {
    pub key: String,
    pub broker_url: String,
    pub topic: String,
    pub client_id: String,
    pub qos: u8,
}

pub struct MetadataStorage {
    db: Database,
    db_path: PathBuf,
}

impl MetadataStorage {
    /// Open (or create) the metadata namespace.
    pub fn open(base_dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let db_path = Self::db_path(base_dir.as_ref());
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent).map_err(StorageError::io)?;
        }

        let db = if db_path.exists() {
            Database::builder()
                .open(db_path.as_path())
                .map_err(StorageError::backend)?
        } else {
            Database::builder()
                .create(db_path.as_path())
                .map_err(StorageError::backend)?
        };

        let storage = Self {
            db,
            db_path: db_path.clone(),
        };
        storage.ensure_tables()?;
        Ok(storage)
    }

    pub fn namespace(&self) -> StorageNamespace {
        StorageNamespace::Metadata
    }

    pub fn path(&self) -> &Path {
        &self.db_path
    }

    pub fn create_stream(&self, stream: StoredStream) -> Result<(), StorageError> {
        self.insert_if_absent(STREAMS_TABLE, &stream.id, &stream)
    }

    pub fn get_stream(&self, id: &str) -> Result<Option<StoredStream>, StorageError> {
        self.get_entry(STREAMS_TABLE, id)
    }

    pub fn delete_stream(&self, id: &str) -> Result<(), StorageError> {
        self.delete_entry(STREAMS_TABLE, id)
    }

    pub fn create_pipeline(&self, pipeline: StoredPipeline) -> Result<(), StorageError> {
        self.insert_if_absent(PIPELINES_TABLE, &pipeline.id, &pipeline)
    }

    pub fn get_pipeline(&self, id: &str) -> Result<Option<StoredPipeline>, StorageError> {
        self.get_entry(PIPELINES_TABLE, id)
    }

    pub fn delete_pipeline(&self, id: &str) -> Result<(), StorageError> {
        self.delete_entry(PIPELINES_TABLE, id)
    }

    pub fn create_mqtt_config(&self, config: StoredMqttClientConfig) -> Result<(), StorageError> {
        self.insert_if_absent(SHARED_MQTT_CONFIGS_TABLE, &config.key, &config)
    }

    pub fn get_mqtt_config(
        &self,
        key: &str,
    ) -> Result<Option<StoredMqttClientConfig>, StorageError> {
        self.get_entry(SHARED_MQTT_CONFIGS_TABLE, key)
    }

    pub fn delete_mqtt_config(&self, key: &str) -> Result<(), StorageError> {
        self.delete_entry(SHARED_MQTT_CONFIGS_TABLE, key)
    }

    fn db_path(base_dir: &Path) -> PathBuf {
        base_dir.join("metadata.redb")
    }

    fn ensure_tables(&self) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(StorageError::backend)?;
        txn.open_table(STREAMS_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(PIPELINES_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(SHARED_MQTT_CONFIGS_TABLE)
            .map_err(StorageError::backend)?;
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    fn insert_if_absent<T: Serialize>(
        &self,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
        value: &T,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(StorageError::backend)?;
        {
            let mut table = txn.open_table(table).map_err(StorageError::backend)?;
            if table.get(key).map_err(StorageError::backend)?.is_some() {
                return Err(StorageError::AlreadyExists(key.to_string()));
            }
            let encoded = encode_record(value)?;
            table
                .insert(key, encoded.as_slice())
                .map_err(StorageError::backend)?;
        }
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    fn get_entry<T: DeserializeOwned>(
        &self,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
    ) -> Result<Option<T>, StorageError> {
        let txn = self.db.begin_read().map_err(StorageError::backend)?;
        let table = txn.open_table(table).map_err(StorageError::backend)?;
        let result = match table.get(key).map_err(StorageError::backend)? {
            Some(value) => {
                let raw = value.value();
                Some(decode_record(raw)?)
            }
            None => None,
        };
        Ok(result)
    }

    fn delete_entry(
        &self,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(StorageError::backend)?;
        {
            let mut table = txn.open_table(table).map_err(StorageError::backend)?;
            let removed = table.remove(key).map_err(StorageError::backend)?;
            if removed.is_none() {
                return Err(StorageError::NotFound(key.to_string()));
            }
        }
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }
}

/// Facade that exposes storage capabilities; currently only metadata, future namespaces can be added.
pub struct StorageManager {
    metadata: MetadataStorage,
    base_dir: PathBuf,
}

impl StorageManager {
    /// Create a storage manager; initializes the metadata namespace under base_dir/metadata.
    pub fn new(base_dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let metadata = MetadataStorage::open(base_dir.join("metadata"))?;
        Ok(Self { metadata, base_dir })
    }

    pub fn metadata(&self) -> &MetadataStorage {
        &self.metadata
    }

    pub fn create_stream(&self, stream: StoredStream) -> Result<(), StorageError> {
        self.metadata.create_stream(stream)
    }

    pub fn get_stream(&self, id: &str) -> Result<Option<StoredStream>, StorageError> {
        self.metadata.get_stream(id)
    }

    pub fn delete_stream(&self, id: &str) -> Result<(), StorageError> {
        self.metadata.delete_stream(id)
    }

    pub fn create_pipeline(&self, pipeline: StoredPipeline) -> Result<(), StorageError> {
        self.metadata.create_pipeline(pipeline)
    }

    pub fn get_pipeline(&self, id: &str) -> Result<Option<StoredPipeline>, StorageError> {
        self.metadata.get_pipeline(id)
    }

    pub fn delete_pipeline(&self, id: &str) -> Result<(), StorageError> {
        self.metadata.delete_pipeline(id)
    }

    pub fn create_mqtt_config(&self, config: StoredMqttClientConfig) -> Result<(), StorageError> {
        self.metadata.create_mqtt_config(config)
    }

    pub fn get_mqtt_config(
        &self,
        key: &str,
    ) -> Result<Option<StoredMqttClientConfig>, StorageError> {
        self.metadata.get_mqtt_config(key)
    }

    pub fn delete_mqtt_config(&self, key: &str) -> Result<(), StorageError> {
        self.metadata.delete_mqtt_config(key)
    }
}

fn encode_record<T: Serialize>(value: &T) -> Result<Vec<u8>, StorageError> {
    let mut buf = Vec::with_capacity(128);
    buf.push(CURRENT_VERSION);
    bincode::serialize_into(&mut buf, value).map_err(StorageError::serialization)?;
    Ok(buf)
}

fn decode_record<T: DeserializeOwned>(raw: &[u8]) -> Result<T, StorageError> {
    if raw.is_empty() {
        return Err(StorageError::Corrupted("empty record"));
    }
    let version = raw[0];
    if version != CURRENT_VERSION {
        return Err(StorageError::UnsupportedVersion(version));
    }
    bincode::deserialize(&raw[1..]).map_err(StorageError::serialization)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    fn sample_stream() -> StoredStream {
        StoredStream {
            id: "stream_1".to_string(),
            stream_type: StoredStreamType::Mqtt,
            schema_json: r#"{"fields":[{"name":"a","type":"int"}]}"#.to_string(),
            props: StoredStreamProps::Mqtt(StoredMqttStreamProps {
                broker_url: "tcp://localhost:1883".to_string(),
                topic: "foo/bar".to_string(),
                qos: 0,
                client_id: Some("cid".to_string()),
                connector_key: None,
            }),
            decoder_type: "json".to_string(),
        }
    }

    fn sample_pipeline() -> StoredPipeline {
        StoredPipeline {
            id: "pipe_1".to_string(),
            sql: "SELECT * FROM stream_1".to_string(),
            sinks: vec![StoredSink {
                id: "sink_1".to_string(),
                kind: "mqtt".to_string(),
                config_json: json!({"topic": "out", "qos": 1}).to_string(),
            }],
        }
    }

    fn sample_mqtt_config() -> StoredMqttClientConfig {
        StoredMqttClientConfig {
            key: "shared_a".to_string(),
            broker_url: "tcp://localhost:1883".to_string(),
            topic: "foo/bar".to_string(),
            client_id: "client_a".to_string(),
            qos: 1,
        }
    }

    #[test]
    fn stream_pipeline_mqtt_roundtrip() {
        let dir = tempdir().unwrap();
        let storage = MetadataStorage::open(dir.path()).unwrap();

        let stream = sample_stream();
        storage.create_stream(stream.clone()).unwrap();
        assert_eq!(
            storage.get_stream(&stream.id).unwrap(),
            Some(stream.clone())
        );

        let pipeline = sample_pipeline();
        storage.create_pipeline(pipeline.clone()).unwrap();
        assert_eq!(
            storage.get_pipeline(&pipeline.id).unwrap(),
            Some(pipeline.clone())
        );

        let mqtt = sample_mqtt_config();
        storage.create_mqtt_config(mqtt.clone()).unwrap();
        assert_eq!(
            storage.get_mqtt_config(&mqtt.key).unwrap(),
            Some(mqtt.clone())
        );

        storage.delete_stream(&stream.id).unwrap();
        storage.delete_pipeline(&pipeline.id).unwrap();
        storage.delete_mqtt_config(&mqtt.key).unwrap();

        assert!(storage.get_stream(&stream.id).unwrap().is_none());
        assert!(storage.get_pipeline(&pipeline.id).unwrap().is_none());
        assert!(storage.get_mqtt_config(&mqtt.key).unwrap().is_none());
    }

    #[test]
    fn create_fails_on_duplicate_keys() {
        let dir = tempdir().unwrap();
        let storage = MetadataStorage::open(dir.path()).unwrap();

        let stream = sample_stream();
        storage.create_stream(stream.clone()).unwrap();
        match storage.create_stream(stream) {
            Err(StorageError::AlreadyExists(id)) => assert_eq!(id, "stream_1"),
            other => panic!("expected AlreadyExists, got {other:?}"),
        }
    }

    #[test]
    fn storage_manager_forwards_to_metadata() {
        let dir = tempdir().unwrap();
        let manager = StorageManager::new(dir.path()).unwrap();

        let stream = sample_stream();
        manager.create_stream(stream.clone()).unwrap();
        assert!(manager.get_stream(&stream.id).unwrap().is_some());

        let pipeline = sample_pipeline();
        manager.create_pipeline(pipeline.clone()).unwrap();
        assert!(manager.get_pipeline(&pipeline.id).unwrap().is_some());

        let mqtt = sample_mqtt_config();
        manager.create_mqtt_config(mqtt.clone()).unwrap();
        assert!(manager.get_mqtt_config(&mqtt.key).unwrap().is_some());

        manager.delete_stream(&stream.id).unwrap();
        manager.delete_pipeline(&pipeline.id).unwrap();
        manager.delete_mqtt_config(&mqtt.key).unwrap();
    }
}
