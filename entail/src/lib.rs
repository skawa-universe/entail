pub mod ds;
pub use entail_derive::Entail;

use std::{borrow::Cow, fmt};

pub trait EntityModel: Sized {
    fn to_ds_entity(&self) -> Result<ds::Entity, EntailError>;
    fn from_ds_entity(e: &ds::Entity) -> Result<Self, EntailError>;
}

#[derive(Debug, Default)]
pub struct EntailError {
    pub message: std::borrow::Cow<'static, str>,
    pub ds_error: Option<google_datastore1::Error>,
}
