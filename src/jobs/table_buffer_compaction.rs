use crate::{metadata::MetadataStore, storage::ObjectStore};

use super::JobError;


pub async fn execute(
    metadata_store: MetadataStore,
    object_store: ObjectStore,
    job_id: i64,
    table_name: String,
) -> Result<(), JobError> {
    todo!()
}