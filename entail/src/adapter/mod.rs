use std::marker::PhantomData;
use std::borrow::Cow;

use super::ds;

pub struct EntityAdapter<T> {
    kind: &'static str,
    _marker: PhantomData<T>,
}

impl<T> EntityAdapter<T> {
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
}
