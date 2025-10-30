use std::borrow::Cow;
use std::marker::PhantomData;

use crate::{EntityModel, EntailError};
use crate::ds;

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
    pub const fn new(kind: &'static str) -> Self {
        Self {
            kind,
            _marker: PhantomData,
        }
    }

    pub fn create_named_key(&self, name: impl Into<Cow<'static, str>>) -> ds::Key {
        self.create_key().with_name(name)
    }

    pub fn create_id_key(&self, id: i64) -> ds::Key {
        self.create_key().with_id(id)
    }

    pub fn create_key(&self) -> ds::Key {
        ds::Key::new(self.kind)
    }

    /// Fetches a single entity and automatically maps
    pub async fn fetch_single(&self, ds: &ds::DatastoreShell, key: ds::Key) -> Result<T, EntailError> {
        let key_string = key.to_string();
        ds.get_single(key).await
            .transpose()
            .unwrap_or_else(|| {
                Err(EntailError::simple(format!("Required {} not found", key_string)))
            })
            .and_then(|e| { T::from_ds_entity(&e) })
    }
}
