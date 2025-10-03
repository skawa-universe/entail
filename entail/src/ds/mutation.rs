use super::*;

pub enum Mutation {
    Insert(Entity),
    Delete(Key),
    Update(Entity),
    Upsert(Entity),
}

impl Into<google_datastore1::api::Mutation> for Mutation {
    fn into(self) -> google_datastore1::api::Mutation {
        match self {
            Mutation::Insert(entity) => google_datastore1::api::Mutation {
                insert: Some(entity.into()),
                ..Default::default()
            },
            Mutation::Delete(key) => google_datastore1::api::Mutation {
                delete: Some(key.into()),
                ..Default::default()
            },
            Mutation::Update(entity) => google_datastore1::api::Mutation {
                update: Some(entity.into()),
                ..Default::default()
            },
            Mutation::Upsert(entity) => google_datastore1::api::Mutation {
                upsert: Some(entity.into()),
                ..Default::default()
            },
        }
    }
}

pub struct MutationResponse {
    pub mutation_results: Vec<MutationResult>,
    pub index_updates: i32,
    pub commit_time: Option<chrono::DateTime<chrono::offset::Utc>>,
}

impl From<google_datastore1::api::CommitResponse> for MutationResponse {
    fn from(value: google_datastore1::api::CommitResponse) -> Self {
        Self {
            mutation_results: value
                .mutation_results
                .unwrap_or_default()
                .into_iter()
                .map(|e| e.into())
                .collect(),
            index_updates: value.index_updates.unwrap_or_default(),
            commit_time: value.commit_time,
        }
    }
}

pub struct MutationResult {
    pub key: Option<Key>,
    pub version: i64,
}

impl From<google_datastore1::api::MutationResult> for MutationResult {
    fn from(value: google_datastore1::api::MutationResult) -> Self {
        Self {
            key: value.key.map(|key| key.into()),
            version: value.version.unwrap_or_default(),
        }
    }
}

pub struct MutationBatch {
    pub mutations: Vec<google_datastore1::api::Mutation>,
}

impl MutationBatch {
    pub fn new() -> Self {
        Self {
            mutations: Vec::new(),
        }
    }

    pub fn add(self, mutation: Mutation) -> Self {
        let mut mutations = self.mutations;
        mutations.push(mutation.into());
        Self { mutations, ..self }
    }
}

impl<'a> Into<Vec<google_datastore1::api::Mutation>> for MutationBatch {
    fn into(self) -> Vec<google_datastore1::api::Mutation> {
        self.mutations
    }
}
