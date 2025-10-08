/*!
The `entail` crate provides a simplified interface to the Datastore and a convenient way to convert Rust
structs into Google Cloud Datastore entities and back again. It does this by leveraging a procedural
macro, `#[derive(Entail)]`, which automatically generates the necessary `EntityModel` trait
implementation for your structs.

This library is essentially a wrapper around `entail_derive`, re-exporting its `Entail` derive macro.
It also provides its own types for Datastore including `Entity` and `Key`, which are used to
represent data in a Datastore-compatible format.

### The `EntityModel` Trait

For each struct annotated with `#[derive(Entail)]`, the `entail_derive` macro generates an
implementation of the `EntityModel` trait:

* `to_ds_entity`: Converts your Rust struct into an `entail::ds::Entity`.
* `from_ds_entity`: Converts an `entail::ds::Entity` back into your Rust struct.

### The `#[entail]` Attribute

The `#[entail]` attribute is used to customize the behavior of the `#[derive(Entail)]` macro.
It can be applied at both the **struct** level and the **field** level.

---

### Struct-Level Attributes

These attributes are placed on the struct definition to configure global behavior.

* `#[entail(rename_all = "camelCase")]`
    This option specifies a naming convention for all fields within the struct, this `camelCase`
    being the default, an empty string will leave the field names alone by default.
    The generated Datastore property names will follow this convention. Supported
    values are `"camelCase"`, `"snake_case"`, `"PascalCase"`, and the empty string for leaving
    it as-is.

* `#[entail(name = "KindName")]`
    This attribute overrides the default Datastore **Kind** name, which is inferred from the
    struct's name.

---

### Field-Level Attributes

By default, fields are **not** persisted to Datastore unless they have a `#[entail]` attribute.
You can mark a field for mapping by simply adding `#[entail]` to it.

Here are the available options for fields:

* `#[entail(key)]`
    Marks the field as the **primary key** for the entity. A struct must have exactly one primary
    key field. This field's value will be used to populate the `name` or `id` component of the
    `entail::ds::Key`. If a field is named `key` and has the `#[entail]` attribute, it's automatically
    treated as the primary key unless overridden.

* `#[entail(field)]`
    Forces a field to be treated as a regular Datastore property, even if its name or other
    attributes might suggest it's a primary key. This is useful for disambiguation, for example,
    on a field named `key`.

* `#[entail(name = "custom_name")]`
    Overrides the Datastore property name for a specific field. By default, the property name is
    the same as the Rust field name, potentially modified by the `rename_all` struct attribute.

* `#[entail(indexed)]`
    Ensures the field is always indexed in Datastore. This is the **default behavior** for any
    field with a `#[entail]` attribute. You only need to use this to explicitly state that a
    field should be indexed.

* `#[entail(unindexed)]`
    Prevents the field from being indexed. This is useful for large or frequently updated fields
    that don't need to be queried.

* `#[entail(unindexed_nulls)]`
    This option is specifically for `Option<T>` fields. It ensures the field is only indexed if
    its value is `Some(T)`. If the value is `None`, the property is still created with a `Null`
    value but will not be indexed.

---

### Type Mapping

The `entail` library handles the conversion between common Rust types and `entail::ds::Value`s.

| Rust Type | `entail::ds::Value` | Notes |
| :--- | :--- | :--- |
| `String`, `Cow<'static, str>` | `UnicodeString` | |
| `i32`, `i64`, `u32` | `Integer` | 32 bit types are mapped to `i64`. |
| `f32`, `f64` | `FloatingPoint` | |
| `bool` | `Boolean` | |
| `Vec<u8>` | `Blob` | |
| `entail::ds::Key` | `Key` | |
| `Vec<T>` | `Array` | The elements of the vector are mapped to `Value`s. |
| `Option<T>` | `T` or `Null` | A value of `Some(T)` is converted to the corresponding `Value`, while `None` becomes `Value::Null`. On deserialization, `Option<T>` can be populated from `Null`, a single `Value`, or an array of one `Value`. An empty array becomes `None`, and an array with more than one element will result in an error. |
*/
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
