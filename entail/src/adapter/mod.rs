use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::marker::PhantomData;

use crate::ds;
use crate::{EntailError, EntityModel};

/// The `EntityAdapter` provides model-specific utility methods for interacting
/// with the Datastore kind of its type.
///
/// This adapter is automatically generated and made available via the
/// [`EntityModel::adapter`] function for every struct deriving `#[derive(Entail)]`.
pub struct EntityAdapter<T>
where
    T: EntityModel,
{
    kind: &'static str,
    _marker: PhantomData<T>,
}

impl<T> EntityAdapter<T>
where
    T: EntityModel,
{
    /// Creates a new `EntityAdapter` instance.
    ///
    /// This is an internal constant function used by the `#[derive(Entail)]`
    /// macro to construct the static adapter instance.
    ///
    /// ## Parameters
    /// - `kind`: The static string slice representing the Datastore **Kind**
    ///   name for the entity model.
    pub const fn new(kind: &'static str) -> Self {
        Self {
            kind,
            _marker: PhantomData,
        }
    }

    /// Converts a Datastore entity into the target Rust struct `T` by consuming the entity.
    ///
    /// This acts as a consuming wrapper around the core [`EntityModel::from_ds_entity`]
    /// logic.
    ///
    /// ## Parameters
    /// - `entity`: The Datastore entity to consume and convert.
    ///
    /// ## Returns
    /// A [`Result`] containing the populated struct instance `T` or an [`EntailError`]
    /// if the conversion fails (e.g., due to mapping issues).
    pub fn consume_entity(entity: ds::Entity) -> Result<T, EntailError> {
        T::from_ds_entity(&entity)
    }

    /// Creates a new Datastore **Key** for the entity with a **string name**
    /// component.
    ///
    /// This is a convenience wrapper around `create_key().with_name(name)`.
    ///
    /// ## Parameters
    /// - `name`: The unique string identifier for the entity within its Kind.
    ///
    /// ## Returns
    /// A new [`ds::Key`] instance with the model's Kind and the specified name.
    pub fn create_named_key(&self, name: impl Into<Cow<'static, str>>) -> ds::Key {
        self.create_key().with_name(name)
    }

    /// Creates a new Datastore **Key** for the entity with an **integer ID**
    /// component.
    ///
    /// This is a convenience wrapper around `create_key().with_id(id)`.
    ///
    /// ## Parameters
    /// - `id`: The unique integer ID for the entity within its Kind.
    ///
    /// ## Returns
    /// A new [`ds::Key`] instance with the model's Kind and the specified ID.
    pub fn create_id_key(&self, id: i64) -> ds::Key {
        self.create_key().with_id(id)
    }

    /// Creates a new **incomplete** Datastore **Key** for the entity.
    ///
    /// The resulting Key contains only the **Kind** component, which is derived
    /// from the struct name or the `#[entail(name = "...")]` attribute.
    /// This is typically used as a base for creating complete Keys with
    /// `with_name()` or `with_id()`.
    ///
    /// ## Returns
    /// A new, incomplete [`ds::Key`] instance for the model's Kind.
    pub fn create_key(&self) -> ds::Key {
        ds::Key::new(self.kind)
    }

    /// Creates a base Datastore **Query** object targeting this entity's **Kind**.
    ///
    /// The returned query is the starting point for building more complex
    /// queries (e.g., adding filters, limits, and orders) that target this model.
    ///
    /// ## Returns
    /// A [`ds::Query`] object pre-configured with the model's Kind.
    pub fn query(&self) -> ds::Query {
        ds::Query {
            kind: self.kind.into(),
            ..ds::Query::default()
        }
    }

    /// Fetches a single entity from Datastore using the provided **Key** and
    /// automatically maps the result to an instance of the Rust struct **T**.
    ///
    /// If no entity is found for the given key, an [`EntailError`] indicating
    /// the entity was not found is returned.
    ///
    /// ## Parameters
    /// - `ds`: A reference to the Datastore client shell.
    /// - `key`: The complete [`ds::Key`] of the entity to fetch.
    ///
    /// ## Returns
    /// A [`Result`] containing the populated struct instance **T** on success,
    /// or an [`EntailError`] if the entity is not found, or if the
    /// deserialization via [`EntityModel::from_ds_entity`] fails.
    pub async fn fetch_single(
        &self,
        ds: &ds::DatastoreShell,
        key: ds::Key,
    ) -> Result<T, EntailError> {
        let key_string = key.to_string();
        ds.get_single(key)
            .await
            .transpose()
            .unwrap_or_else(|| {
                Err(EntailError::simple(
                    crate::EntailErrorKind::RequiredEntityNotFound,
                    format!("Required {} not found", key_string),
                ))
            })
            .and_then(|e| T::from_ds_entity(&e))
    }

    /// Fetches a batch of entities from Datastore using the provided `keys` and
    /// automatically maps the results to instances of the Rust struct `T`.
    ///
    /// The result is returned as a `HashMap` where the **complete Key** is mapped to the
    /// successfully deserialized struct `T`. Entities that are **not found** in Datastore
    /// are simply omitted from the resulting map.
    ///
    /// ## Parameters
    /// - `ds`: A reference to the Datastore client shell.
    /// - `keys`: A collection of complete [`ds::Key`]s to fetch. This parameter is highly flexible:
    ///   * You can pass a **container of keys** (e.g., `Vec<Key>`) to consume the container and all
    ///     keys within it.
    ///   * You can pass an **address of a container** (e.g., `&[Key]`) to keep the container and
    ///     the keys.
    ///   * You can pass a **container of key references** (e.g., `Vec<&Key>`) where the container
    ///     itself is consumed, but the referenced key objects are retained by the caller.
    ///
    /// ## Returns
    /// A [`Result`] containing a `HashMap<ds::Key, T>` on success, or an [`EntailError`]
    /// if the batch fetch fails or if any *found* entity fails the deserialization
    /// process via [`EntityModel::from_ds_entity`].
    pub async fn fetch_all<I>(
        &self,
        ds: &ds::DatastoreShell,
        keys: I,
    ) -> Result<HashMap<ds::Key, T>, EntailError>
    where
        I: IntoIterator,
        I::Item: Borrow<ds::Key>,
    {
        let result = ds.get_all(keys).await?;
        let mut map = HashMap::with_capacity(result.len());
        for entity in result.into_iter() {
            let model = T::from_ds_entity(&entity)?;
            let key = entity.just_key();
            map.insert(key, model);
        }
        Ok(map)
    }

    /// Executes a Datastore query and automatically maps all resulting entities to the struct `T`.
    ///
    /// This function performs the query execution and then uses the `consume_entity`
    /// function to map every fetched entity to the model type `T`.
    ///
    /// ## Parameters
    /// - `ds`: A reference to the Datastore client shell.
    /// - `query`: The complete [`ds::Query`] definition to execute.
    ///
    /// ## Returns
    /// A [`Result`] containing a [`ds::QueryResult`] where the entities are instances of `T`,
    /// or an [`EntailError`] if the query fails or any entity mapping fails.
    pub async fn fetch_query(
        &self,
        ds: &ds::DatastoreShell,
        query: ds::Query,
    ) -> Result<ds::QueryResult<T>, EntailError> {
        ds.run_query(query)
            .await
            .and_then(|query_result| query_result.try_map(Self::consume_entity))
    }
}
