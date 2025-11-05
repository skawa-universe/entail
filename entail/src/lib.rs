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

* `#[entail(text)]`
    This option specifies that the string field should be encoded as a **large block of text**.
    This is primarily for **compatibility with App Engine Standard Java clients** (by setting
    the property's internal `meaning` to `entail::ds::MEANING_TEXT`). Cloud Datastore does not
    strictly require this flag for long strings, as any unindexed string property can store
    values up to 1 MiB. However, this flag explicitly marks the field for correct decoding as a
    Text type in older environments. **Text properties are always unindexed.**

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
mod adapter;

use std::{borrow::Cow, fmt};

/// A trait automatically implemented for structs that derive `#[derive(Entail)]`.
///
/// This trait provides the core functionality for converting between a Rust struct
/// and a Cloud Datastore entity representation, enabling seamless persistence.
pub trait EntityModel: Sized {
    /// Converts the Rust struct instance into an `entail::Entity` (aliased as `ds::Entity`).
    ///
    /// This method maps the struct's fields to Datastore properties, applying any
    /// field-level configurations like renaming or indexing.
    ///
    /// ## Returns
    /// A [`Result`] containing the Datastore [`ds::Entity`] or an [`EntailError`]
    /// if the conversion fails (e.g., due to an incompatible type).
    fn to_ds_entity(&self) -> Result<ds::Entity, EntailError>;

    /// Converts a Datastore entity reference back into an instance of the Rust struct.
    ///
    /// This method is responsible for validating and extracting properties from the
    /// entity and mapping them back to the struct's fields.
    ///
    /// ## Parameters
    /// - `e`: A reference to the Datastore entity.
    ///
    /// ## Returns
    /// A [`Result`] containing the populated struct instance or an [`EntailError`]
    /// if the conversion fails (e.g., a required field is missing or a type mismatch occurs).
    fn from_ds_entity(e: &ds::Entity) -> Result<Self, EntailError>;

    /// Returns a static reference to the EntityAdapter for type T.
    ///
    /// This adapter provides utility methods (like key creation) tied to the model.
    fn adapter() -> &'static EntityAdapter<Self>;
}

/// Represents the high-level category of error that occurred.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum EntailErrorKind {
    /// An error of an indeterminate or unexpected nature.
    Unknown,
    /// The operation required an entity to exist in the Datastore (e.g., `fetch_single`), but it was **not found**.
    RequiredEntityNotFound,
    /// The underlying **Cloud Datastore API call** failed. This kind of error often
    /// has an attached `ds_error` field providing the specific API context.
    RequestFailure,
    /// The transaction or operation was retried the maximum allowed times, but still did **not succeed**
    /// (e.g., due to repeated contention or conflicts).
    RetriesExhausted,
    /// The **kind** of the entity being deserialized does not match the **kind** expected by the target Rust struct.
    /// This occurs in `EntityModel::from_ds_entity`.
    EntityKindMismatch,
    /// An error occurred during the conversion process between an entity's properties and the Rust struct's fields,
    /// such as a **type mismatch** or a **missing required property**.
    PropertyMappingError,
}

impl Default for EntailErrorKind {
    fn default() -> Self {
        Self::Unknown
    }
}

/// The primary error type used throughout the `entail` crate for operations that can fail.
///
/// This error encapsulates both logic errors within the library (such as data
/// validation failures during mapping) and underlying Cloud Datastore API errors.
#[derive(Debug, Default)]
pub struct EntailError {
    /// The general category of the error, indicating where in the process the failure occurred.
    pub kind: EntailErrorKind,
    /// A human-readable message describing the nature of the error on the Entail level.
    pub message: std::borrow::Cow<'static, str>,
    /// An optional underlying error returned directly by the `google-datastore1`
    /// client library, providing detailed context for API failures (e.g., networking,
    /// authorization, or transactional conflicts).
    pub ds_error: Option<google_datastore1::Error>,
}

impl EntailError {
    /// Creates a new `EntailError` with a specified `kind` and message, leaving `ds_error` as `None`.
    ///
    /// This is typically used for library-level or mapping errors that don't originate
    /// from a low-level Datastore API call failure.
    pub fn simple(
        kind: EntailErrorKind,
        message: impl Into<std::borrow::Cow<'static, str>>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            ds_error: None,
        }
    }
}

pub use adapter::*;
