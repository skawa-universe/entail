use crate::{EntailError, EntityModel, ds::Entity};

/// A container that synchronizes a Rust model with its underlying Datastore [`Entity`].
///
/// `ModeledUpdate` is designed to facilitate safe updates to entities where the Rust model 
/// might not represent every property stored in Datastore. By holding both the model and 
/// the original entity, it allows you to modify modeled fields while preserving 
/// unmodeled properties.
pub struct ModeledUpdate<T: EntityModel> {
    /// The strongly-typed Rust representation of the entity.
    pub model: T,
    /// The raw Datastore entity, used to preserve properties not captured by the model.
    pub entity: Entity,
}

impl<T: EntityModel> ModeledUpdate<T> {
    /// Creates a new `ModeledUpdate` by deserializing the provided [`Entity`] into the model `T`.
    ///
    /// ## Parameters
    /// - `entity`: The raw Datastore entity fetched from the server.
    ///
    /// ## Returns
    /// A [`Result`] containing the `ModeledUpdate` instance, or an [`EntailError`] if 
    /// the entity cannot be mapped to the model.
    pub fn new(entity: Entity) -> Result<ModeledUpdate<T>, EntailError> {
        T::from_ds_entity(&entity).map(|model| ModeledUpdate { model, entity })
    }

    /// Synchronizes the internal `entity` with the current state of the `model`.
    ///
    /// This method converts the model back into an entity and merges its properties into 
    /// the existing raw entity. This ensures that properties defined in the 
    /// model are updated, while any "extra" properties already present in `self.entity` 
    /// remain untouched.
    ///
    /// ## Returns
    /// A [`Result`] containing a reference to the updated [`Entity`] ready for commit, 
    /// or an [`EntailError`] if serialization fails.
    pub fn update_entity(&mut self) -> Result<&Entity, EntailError> {
        self.entity.consume_properties_from(self.model.to_ds_entity()?);
        Ok(&self.entity)
    }

    /// Synchronizes the model with the entity and returns the resulting [`Entity`], consuming this container.
    ///
    /// This is the preferred method for the final step of a "fetch-modify-update" cycle. 
    /// It performs the property sync via [`Self::update_entity`] and then returns the 
    /// underlying entity, making it ready to be passed into a mutation for commitment.
    ///
    /// ## Returns
    /// A [`Result`] containing the fully updated [`Entity`] or an [`EntailError`] 
    /// if serialization fails.
    pub fn update_into_entity(mut self) -> Result<Entity, EntailError> {
        self.update_entity()?;
        Ok(self.entity)
    }
}
