use super::*;

/// Represents a single mutation operation to be applied to the Datastore.
///
/// Mutations are grouped into a [`MutationBatch`] and committed to the Datastore
/// using [`DatastoreShell::commit`].
#[derive(Debug)]
pub enum Mutation {
    /// Inserts a new entity into the Datastore.
    ///
    /// The operation will fail if an entity with the specified key already exists.
    Insert(Entity),
    /// Deletes an entity from the Datastore using its key.
    ///
    /// This operation is idempotent: it will not fail even if the entity
    /// corresponding to the key does not exist.
    Delete(Key),
    /// Updates an existing entity in the Datastore.
    ///
    /// The operation will fail if an entity with the specified key does not exist.
    Update(Entity),
    /// Writes an entity to the Datastore, either by creating a new one or
    /// replacing an existing one with the same key.
    ///
    /// The operation succeeds regardless of whether the entity existed prior
    /// to the mutation.
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

/// The response for [`DatastoreShell::commit`]
#[derive(Debug, Default)]
pub struct MutationResponse {
    /// The result of performing the mutations. The i-th mutation result corresponds
    /// to the i-th mutation in the request.
    pub mutation_results: Vec<MutationResult>,
    /// The number of index entries updated during the commit,
    /// or zero if none were updated.
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

/// The result of applying a mutation.
#[derive(Debug)]
pub struct MutationResult {
    /// The automatically allocated key. Set only when the mutation allocated a key.
    pub key: Option<Key>,
    /// The version of the entity on the server after processing the mutation.
    /// If the mutation doesn't change anything on the server, then the version
    /// will be the version of the current entity or, if no entity is present,
    /// a version that is strictly greater than the version of any previous
    /// entity and less than the version of any possible future entity.
    pub version: i64,
    /// The create time of the entity.
    /// This field will not be set after a [`Mutation::Delete`].
    pub create_time: Option<chrono::DateTime<chrono::offset::Utc>>,
    /// The update time of the entity on the server after processing the mutation.
    /// If the mutation doesn't change anything on the server, then the timestamp
    /// will be the update timestamp of the current entity. This field will not be
    /// set after a [`Mutation::Delete`].
    pub update_time: Option<chrono::DateTime<chrono::offset::Utc>>,
}

impl From<google_datastore1::api::MutationResult> for MutationResult {
    fn from(value: google_datastore1::api::MutationResult) -> Self {
        Self {
            key: value.key.map(|key| key.into()),
            version: value.version.unwrap_or_default(),
            create_time: value.create_time,
            update_time: value.update_time,
        }
    }
}

/// Represents a batch of mutations to be applied to the Datastore
#[derive(Debug)]
pub struct MutationBatch {
    pub mutations: Vec<google_datastore1::api::Mutation>,
}

impl MutationBatch {
    /// Creates a new, empty `MutationBatch` instance.
    pub fn new() -> Self {
        Self {
            mutations: Vec::new(),
        }
    }

    /// Adds a [`Mutation`] to the batch.
    ///
    /// This method consumes `self` and returns the updated batch, allowing for
    /// chaining of calls.
    ///
    /// ## Parameters
    /// - `mutation`: The specific mutation operation (Insert, Delete, Update, or Upsert) to add.
    pub fn add(self, mutation: Mutation) -> Self {
        let mut mutations = self.mutations;
        mutations.push(mutation.into());
        Self { mutations, ..self }
    }

    /// Adds a collection of [`Mutation`]s to the batch.
    ///
    /// This method consumes `self` and returns the updated batch, allowing for
    /// chaining of calls.
    ///
    /// ## Parameters
    /// - `new_mutations`: An iterable of mutation operations to add.
    pub fn add_all<I>(self, new_mutations: I) -> Self
    where
        I: IntoIterator<Item = Mutation>
    {
        let mut mutations = self.mutations;
        mutations.extend(new_mutations.into_iter().map(Into::into));
        Self { mutations, ..self }
    }

    /// Convenience method to add an [`Mutation::Insert`] operation.
    ///
    /// The entity must not already exist in the Datastore for the operation to succeed.
    pub fn insert(self, e: Entity) -> Self {
        self.add(Mutation::Insert(e))
    }

    /// Convenience method to add an [`Mutation::Update`] operation.
    ///
    /// The entity must already exist in the Datastore for the operation to succeed.
    pub fn update(self, e: Entity) -> Self {
        self.add(Mutation::Update(e))
    }

    /// Convenience method to add an [`Mutation::Upsert`] operation.
    ///
    /// This will either insert a new entity or overwrite an existing one.
    pub fn upsert(self, e: Entity) -> Self {
        self.add(Mutation::Upsert(e))
    }

    /// Convenience method to add an [`Mutation::Delete`] operation.
    ///
    /// Deletes the entity specified by the [`Key`]. The operation will not fail
    /// if the entity does not exist.
    pub fn delete(self, key: Key) -> Self {
        self.add(Mutation::Delete(key))
    }

    /// Convenience method to add multiple [`Mutation::Insert`] operations.
    ///
    /// Entities in the iterable must not already exist in the Datastore for the operation to succeed.
    pub fn insert_all<I>(self, entities: I) -> Self
    where
        I: IntoIterator<Item = Entity>
    {
        self.add_all(entities.into_iter().map(Mutation::Insert))
    }

    /// Convenience method to add multiple [`Mutation::Update`] operations.
    ///
    /// Entities in the iterable must already exist in the Datastore for the operation to succeed.
    pub fn update_all<I>(self, entities: I) -> Self
    where
        I: IntoIterator<Item = Entity>
    {
        self.add_all(entities.into_iter().map(Mutation::Update))
    }

    /// Convenience method to add multiple [`Mutation::Upsert`] operations.
    ///
    /// This will either insert new entities or overwrite existing ones.
    pub fn upsert_all<I>(self, entities: I) -> Self
    where
        I: IntoIterator<Item = Entity>
    {
        self.add_all(entities.into_iter().map(Mutation::Upsert))
    }

    /// Convenience method to add multiple [`Mutation::Delete`] operations.
    ///
    /// Deletes the entities specified by the iterable of [`Key`]s. The operations
    /// will not fail if any of the entities do not exist.
    pub fn delete_all<I>(self, keys: I) -> Self
    where
        I: IntoIterator<Item = Key>
    {
        self.add_all(keys.into_iter().map(Mutation::Delete))
    }
}

impl<'a> Into<Vec<google_datastore1::api::Mutation>> for MutationBatch {
    fn into(self) -> Vec<google_datastore1::api::Mutation> {
        self.mutations
    }
}
