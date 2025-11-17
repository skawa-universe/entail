use super::super::*;
use std::collections::HashMap;

/// Represents the specific variant of the last path element of a Datastore Key.
///
/// A key is either incomplete (no ID/name), named, or identified by an integer ID.
#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum KeyVariant {
    /// A string name component for the key path element.
    Name(Cow<'static, str>),
    /// An integer ID component for the key path element.
    Id(i64),
    /// An incomplete key, meaning it has a kind but neither an ID nor a name.
    Incomplete,
}

/// A representation of a Google Cloud Datastore Key.
///
/// This structure encapsulates the **kind** of the entity, its **ID or name**,
/// and an optional **parent Key** to establish entity hierarchy.
#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub struct Key {
    kind: Cow<'static, str>,
    variant: KeyVariant,
    parent: Option<Box<Key>>,
}

impl Key {
    /// Creates a new **incomplete** Key with only the specified kind.
    ///
    /// This key cannot be used to fetch or update an entity but is the base for
    /// creating complete Keys.
    ///
    /// ## Parameters
    /// - `kind`: The Datastore kind name (e.g., `"User"`, `"Product"`).
    pub fn new(kind: impl Into<Cow<'static, str>>) -> Self {
        Key {
            kind: kind.into(),
            variant: KeyVariant::Incomplete,
            parent: None,
        }
    }

    /// Gets a string slice reference to the kind of the entity represented by this Key.
    pub fn kind(&self) -> &str {
        self.kind.as_ref()
    }

    /// Gets the string name component of the Key, if it has one.
    pub fn name(&self) -> Option<&str> {
        if let KeyVariant::Name(name) = &self.variant {
            Some(name.as_ref())
        } else {
            None
        }
    }

    /// Gets the integer ID component of the Key, if it has one.
    pub fn id(&self) -> Option<i64> {
        if let KeyVariant::Id(id) = &self.variant {
            Some(*id)
        } else {
            None
        }
    }

    /// Gets a reference to the parent Key, if this Key is part of a key path.
    pub fn parent(&self) -> Option<&Key> {
        self.parent.as_deref()
    }

    /// Consumes the current Key and returns a new one with the specified **string name**.
    ///
    /// This replaces any existing ID or name component.
    pub fn with_name(self, name: impl Into<Cow<'static, str>>) -> Self {
        Key {
            variant: KeyVariant::Name(name.into()),
            ..self
        }
    }

    /// Consumes the current Key and returns a new one with the specified **integer ID**.
    ///
    /// This replaces any existing ID or name component.
    pub fn with_id(self, id: i64) -> Self {
        Key {
            variant: KeyVariant::Id(id),
            ..self
        }
    }

    /// Consumes the current Key and returns a new one with a single parent Key.
    ///
    /// The parent Key is boxed internally.
    pub fn with_parent(self, parent: Key) -> Self {
        Key {
            parent: Some(Box::new(parent)),
            ..self
        }
    }

    /// Consumes the current Key and returns a new one with no parent.
    ///
    /// This clears any existing parent Key.
    pub fn with_no_parent(self) -> Self {
        Key {
            parent: None,
            ..self
        }
    }

    /// Convenience method that consumes the current Key and returns a new one with an optional boxed parent.
    pub fn with_boxed_parent(self, parent: Option<Box<Key>>) -> Self {
        Key {
            parent: parent,
            ..self
        }
    }

    /// Converts this `entail::ds::Key` reference into the lower-level
    /// `google_datastore1::api::Key` representation.
    pub fn to_api(&self) -> google_datastore1::api::Key {
        let mut path = Vec::new();
        self.push_path_elements(&mut path);
        google_datastore1::api::Key {
            partition_id: None,
            path: Some(path),
        }
    }

    /// Recursively traverses the key path (starting from the root parent) and pushes
    /// the path elements (kind + ID/name) into the output vector.
    fn push_path_elements(&self, out: &mut Vec<google_datastore1::api::PathElement>) {
        if let Some(parent) = &self.parent {
            parent.push_path_elements(out);
        }
        out.push(match &self.variant {
            KeyVariant::Id(id) => google_datastore1::api::PathElement {
                kind: Some(self.kind.to_string()),
                id: Some(*id),
                ..Default::default()
            },
            KeyVariant::Name(name) => google_datastore1::api::PathElement {
                kind: Some(self.kind.to_string()),
                name: Some(name.to_string()),
                ..Default::default()
            },
            KeyVariant::Incomplete => google_datastore1::api::PathElement {
                kind: Some(self.kind.to_string()),
                ..Default::default()
            },
        });
    }

    /// Recursively traverses and consumes the key path, pushing owned path elements
    /// into the output vector. Used for `Into<google_datastore1::api::Key>`.
    fn consume_and_push_path_elements(self, out: &mut Vec<google_datastore1::api::PathElement>) {
        if let Some(parent) = &self.parent {
            parent.push_path_elements(out);
        }

        let kind = self.kind.into_owned();
        out.push(match self.variant {
            KeyVariant::Id(id) => google_datastore1::api::PathElement {
                kind: Some(kind),
                id: Some(id),
                ..Default::default()
            },
            KeyVariant::Name(name) => google_datastore1::api::PathElement {
                kind: Some(kind),
                name: Some(name.into_owned()),
                ..Default::default()
            },
            KeyVariant::Incomplete => google_datastore1::api::PathElement {
                kind: Some(kind),
                ..Default::default()
            },
        });
    }
}

impl Into<google_datastore1::api::Key> for Key {
    /// Converts `entail::ds::Key` into the lower-level API `Key` by consuming it.
    fn into(self) -> google_datastore1::api::Key {
        let mut path = Vec::new();
        self.consume_and_push_path_elements(&mut path);
        google_datastore1::api::Key {
            partition_id: None,
            path: Some(path),
        }
    }
}

impl From<google_datastore1::api::Key> for Key {
    /// Converts the lower-level API `Key` into the higher-level `entail::Key`.
    ///
    /// This reconstructs the parent-child key hierarchy from the API's path elements.
    fn from(value: google_datastore1::api::Key) -> Key {
        let mut key_opt = None;
        for element in value.path.expect("Missing key path") {
            let mut key = Key::new(element.kind.expect("Kindless key"));
            if let Some(id) = element.id {
                key = key.with_id(id);
            } else if let Some(name) = element.name {
                key = key.with_name(name)
            }
            if let Some(parent) = key_opt {
                key = key.with_boxed_parent(Some(Box::new(parent)));
            }
            key_opt = Some(key);
        }
        key_opt.expect("Empty path")
    }
}

impl fmt::Display for Key {
    /// Formats the Key into a canonical Datastore-like string representation
    /// (e.g., `ParentKind("name") / ChildKind(id:123)`).
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(pk) = &self.parent {
            pk.fmt(f)?;
            write!(f, "/")?;
        }
        write!(f, "{}(", self.kind)?;
        match &self.variant {
            KeyVariant::Name(name) => {
                let lit = serde_json::to_string(&name).map_err(|_| fmt::Error)?;
                write!(f, "name:{})", lit)
            }
            KeyVariant::Id(id) => {
                write!(f, "id:{})", id)
            }
            KeyVariant::Incomplete => {
                write!(f, ")")
            }
        }
    }
}

/// Represents the various data types that a single Datastore property can hold.
#[derive(PartialEq, Debug, Clone)]
pub enum Value {
    /// Represents the Datastore Null value.
    Null,
    /// An integer value, mapped to `i64`.
    Integer(i64),
    /// A boolean value.
    Boolean(bool),
    /// Binary data.
    Blob(Vec<u8>),
    /// A string value, represented as owned or borrowed static string.
    UnicodeString(Cow<'static, str>),
    /// A floating point value, mapped to `f64`.
    FloatingPoint(f64),
    /// An array/list of other `Value`s.
    Array(Vec<Value>),
    /// A Datastore Key value.
    Key(Key),
}

impl Value {
    /// Creates a `Value::Null`.
    pub fn null() -> Value {
        Value::Null
    }

    /// Creates a `Value::Integer`.
    pub fn integer(val: i64) -> Value {
        Value::Integer(val)
    }

    /// Creates a `Value::Boolean`.
    pub fn boolean(val: bool) -> Value {
        Value::Boolean(val)
    }

    /// Creates a `Value::Blob` from anything that can be converted into `Vec<u8>`.
    pub fn blob(val: impl Into<Vec<u8>>) -> Value {
        Value::Blob(val.into())
    }

    /// Creates a `Value::UnicodeString` from anything that can be converted into `Cow<'static, str>`.
    pub fn unicode_string(s: impl Into<Cow<'static, str>>) -> Value {
        Value::UnicodeString(s.into())
    }

    /// Creates a `Value::FloatingPoint`.
    pub fn floating_point(val: f64) -> Value {
        Value::FloatingPoint(val)
    }

    /// Creates a `Value::Array`.
    pub fn array(val: Vec<Value>) -> Value {
        Value::Array(val)
    }

    /// Creates a `Value::Key`.
    pub fn key(key: Key) -> Value {
        Value::Key(key)
    }

    /// Returns a string slice of the value if it is `UnicodeString`.
    pub fn string_value(&self) -> Option<&str> {
        match self {
            Self::UnicodeString(str) => Some(str.as_ref()),
            _ => None,
        }
    }

    /// Returns a reference to the Key if the value is `Key`.
    pub fn key_value(&self) -> Option<&Key> {
        match self {
            Self::Key(key) => Some(key),
            _ => None,
        }
    }

    /// Returns a byte slice of the value if it is `Blob`.
    pub fn blob_value(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(bytes) => Some(bytes),
            _ => None,
        }
    }

    /// Checks if the value is `Value::Null`.
    pub fn is_null(&self) -> bool {
        match self {
            Self::Null => true,
            _ => false,
        }
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::unicode_string(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::boolean(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::integer(value as i64)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Self::integer(value as i64)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::integer(value)
    }
}

impl From<&'static str> for Value {
    fn from(value: &'static str) -> Self {
        Self::unicode_string(Cow::Borrowed(value))
    }
}

impl From<Cow<'static, str>> for Value {
    fn from(value: Cow<'static, str>) -> Self {
        Self::unicode_string(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::floating_point(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Self::floating_point(value as f64)
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::blob(value)
    }
}

impl From<Vec<Value>> for Value {
    fn from(value: Vec<Value>) -> Self {
        Self::array(value)
    }
}

impl From<Key> for Value {
    fn from(value: Key) -> Self {
        Self::key(value)
    }
}
impl From<google_datastore1::api::Value> for Value {
    /// Converts the lower-level API `Value` into the higher-level `entail::Value`.
    fn from(value: google_datastore1::api::Value) -> Self {
        if let Some(integer_value) = value.integer_value {
            Value::Integer(integer_value)
        } else if let Some(boolean_value) = value.boolean_value {
            Value::Boolean(boolean_value)
        } else if let Some(blob_value) = value.blob_value {
            Value::Blob(blob_value.clone())
        } else if let Some(string_value) = value.string_value {
            Value::UnicodeString(Cow::Owned(string_value))
        } else if let Some(double_value) = value.double_value {
            Value::FloatingPoint(double_value)
        } else if let Some(array_value) = value.array_value {
            let values: Vec<Value> = array_value
                .values
                .unwrap_or_default()
                .into_iter()
                .map(|e| e.into())
                .collect();
            Value::Array(values)
        } else if let Some(key_value) = value.key_value {
            Value::Key(key_value.into())
        } else if value.entity_value.is_some()
            || value.geo_point_value.is_some()
            || value.timestamp_value.is_some()
        {
            // Panic for unsupported types like `entityValue`, `geoPointValue`,
            // `timestampValue`, and others.
            panic!("Unsupported Datastore value type");
        } else {
            // Sometimes Cloud Datastore sends `{}`` as value JSON instead of null, but this
            // branch covers the normal null value case (`{"nullValue": "NULL_VALUE"}``)
            Value::Null
        }
    }
}

impl Into<google_datastore1::api::Value> for Value {
    /// Converts `entail::Value` into the lower-level API `Value` by consuming it.
    fn into(self) -> google_datastore1::api::Value {
        let mut ds_value = google_datastore1::api::Value::default();

        match self {
            Value::Null => {
                // Datastore API requires the string "NULL_VALUE" for null values.
                ds_value.null_value = Some("NULL_VALUE".to_string());
            }
            Value::Integer(i) => {
                ds_value.integer_value = Some(i);
            }
            Value::Boolean(b) => {
                ds_value.boolean_value = Some(b);
            }
            Value::Blob(b) => {
                // Clone Vec<u8>
                ds_value.blob_value = Some(b.clone());
            }
            Value::UnicodeString(s) => {
                // Convert Cow<'static, str> to String
                ds_value.string_value = Some(s.into_owned());
            }
            Value::FloatingPoint(f) => {
                ds_value.double_value = Some(f);
            }
            Value::Key(k) => {
                ds_value.key_value = Some(k.into());
            }
            Value::Array(values) => {
                // Recursively convert inner elements back to DatastoreValue
                let ds_elements = values.into_iter().map(Value::into).collect();
                ds_value.array_value = Some(google_datastore1::api::ArrayValue {
                    values: Some(ds_elements),
                });
            }
        }

        ds_value
    }
}

impl fmt::Display for Value {
    /// Formats the Value for display, showing its type and content (e.g., `int(42)`, `string(hello)`).
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Integer(i) => write!(f, "int({})", i),
            Value::Boolean(b) => write!(f, "bool({})", b),
            Value::Blob(b) => write!(f, "blob(size: {})", b.len()),
            Value::UnicodeString(s) => write!(f, "string({})", s),
            Value::FloatingPoint(d) => write!(f, "float({})", d),
            Value::Array(vals) => {
                write!(f, "[")?;
                for val in vals {
                    write!(f, "{},", val)?;
                }
                write!(f, "]")?;
                Ok(())
            }
            Value::Key(key) => write!(f, "key({})", key),
        }
    }
}

pub static MEANING_TEXT: i32 = 15;

/// Represents a single Datastore property, which includes the `Value`,
/// its **indexing** status, and an optional **meaning** hint.
#[derive(PartialEq, Debug, Clone)]
pub struct PropertyValue {
    value: Value,
    indexed: bool,
    meaning: Option<i32>,
}

impl PropertyValue {
    /// Gets a reference to the raw `Value` held by the property.
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// Returns `true` if the property value is indexed in Datastore.
    pub fn is_indexed(&self) -> bool {
        self.indexed
    }

    /// Gets the optional integer meaning (e.g., used for specific types like geospatial points).
    pub fn meaning(&self) -> Option<i32> {
        self.meaning.clone()
    }
}

impl fmt::Display for PropertyValue {
    /// Formats the property value, including its indexing status.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.value.fmt(f)?;
        if self.indexed {
            write!(f, " (indexed)")
        } else {
            write!(f, " (unindexed)")
        }
    }
}

/// A representation of a Google Cloud Datastore **Entity**.
///
/// It holds the unique `Key` for the entity and a `HashMap` of all its properties.
#[derive(Debug, Clone)]
pub struct Entity {
    key: Key,
    properties: HashMap<Cow<'static, str>, PropertyValue>,
}

impl Entity {
    /// Creates a new Entity with a complete or incomplete Key.
    pub fn new(key: Key) -> Self {
        Self {
            key,
            properties: HashMap::new(),
        }
    }

    /// Creates a new Entity with an incomplete Key of the specified kind.
    pub fn of_kind(kind: impl Into<Cow<'static, str>>) -> Self {
        Self {
            key: Key::new(kind),
            properties: HashMap::new(),
        }
    }

    /// Gets a reference to the entity's unique `Key`.
    pub fn key(&self) -> &Key {
        &self.key
    }

    /// Consumes the entity and returns just the key.
    pub fn just_key(self) -> Key {
        self.key
    }

    /// Gets a string slice reference to the kind of this entity.
    pub fn kind(&self) -> &str {
        self.key.kind()
    }

    /// Returns an iterator over all raw property entries (name and `PropertyValue`).
    pub fn property_iter_raw(&self) -> impl Iterator<Item = (&Cow<'static, str>, &PropertyValue)> {
        self.properties.iter()
    }

    /// Returns an iterator over property names and their raw `Value` (excluding indexing info).
    pub fn property_iter(&self) -> impl Iterator<Item = (&Cow<'static, str>, &Value)> {
        self.properties
            .iter()
            .map(|(key, value)| (key, value.value()))
    }

    /// Sets the Key for the entity. Returns a mutable reference to self.
    pub fn set_key(&mut self, key: Key) -> &mut Self {
        self.key = key;
        self
    }

    /// Sets a property on the entity with full control over indexing and meaning.
    ///
    /// ## Parameters
    /// - `name`: The property name.
    /// - `value`: The property value.
    /// - `indexed`: Whether the property should be indexed.
    /// - `meaning`: An optional meaning hint for the Datastore API.
    pub fn set(
        &mut self,
        name: impl Into<Cow<'static, str>>,
        value: Value,
        indexed: bool,
        meaning: Option<i32>,
    ) -> &mut Self {
        self.properties.insert(
            name.into(),
            PropertyValue {
                value,
                indexed,
                meaning,
            },
        );
        self
    }

    /// Sets a property, forcing it to be **unindexed** (convenience function).
    pub fn set_unindexed(&mut self, name: impl Into<Cow<'static, str>>, value: Value) -> &mut Self {
        self.set(name, value, false, None)
    }

    /// Sets a property, forcing it to be **indexed** (convenience function).
    pub fn set_indexed(&mut self, name: impl Into<Cow<'static, str>>, value: Value) -> &mut Self {
        self.set(name, value, true, None)
    }

    /// Sets a property with advanced control over indexing based on the value's null status.
    ///
    /// **Empty arrays** (`Value::Array` with zero elements) are internally **converted to `Value::Null`**
    /// before determining indexing and applying the `meaning` logic, aligning with Cloud Datastore's convention
    /// of treating them as effectively null for storage.
    ///
    /// ## Parameters
    /// - `name`: The property name.
    /// - `value`: The property value.
    /// - `index_values`: If the effective value is **non-null**, this flag determines whether
    ///   it is indexed.
    /// - `index_nulls`: If the effective value **is** null (or an empty array), this flag
    ///   determines whether it is indexed.
    /// - `meaning`: An optional integer hint (`i32`) for the Datastore API. **Note: This is
    ///   ignored if the effective value is null.**
    pub fn set_advanced(
        &mut self,
        name: impl Into<Cow<'static, str>>,
        value: Value,
        index_values: bool,
        index_nulls: bool,
        meaning: Option<i32>,
    ) -> &mut Self {
        let effective_value = match &value {
            Value::Array(values) => {
                if values.is_empty() {
                    Value::null()
                } else {
                    value
                }
            }
            _ => value,
        };
        let is_null = effective_value.is_null();
        let indexed = if is_null { index_nulls } else { index_values };
        // Null values have no meaning property
        self.set(
            name,
            effective_value,
            indexed,
            if is_null { None } else { meaning },
        )
    }

    /// Checks if a property with the given name is indexed. Returns `false` if the property doesn't exist.
    pub fn is_indexed(&self, name: &str) -> bool {
        self.properties
            .get(name)
            .map(|v| v.indexed)
            .unwrap_or(false)
    }

    /// Gets a reference to the raw `Value` of a property by name.
    pub fn get_value(&self, name: &str) -> Option<&Value> {
        self.properties.get(name).map(|ev| &ev.value)
    }

    /// Gets a reference to the full `PropertyValue` (including indexing) of a property by name.
    pub fn get(&self, name: &str) -> Option<&PropertyValue> {
        self.properties.get(name)
    }
}

impl fmt::Display for Entity {
    /// Formats the Entity, showing its Key and a list of all its properties.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.key.fmt(f)?;
        write!(f, " {{")?;
        for (key, value) in self.properties.iter() {
            write!(f, "\n  {}: {},", key, value)?;
        }
        write!(f, "\n}}")
    }
}

impl From<google_datastore1::api::Entity> for Entity {
    /// Converts the lower-level API `Entity` into the higher-level `entail::ds::Entity`.
    fn from(value: google_datastore1::api::Entity) -> Entity {
        let mut result = Entity::new(value.key.expect("Missing key").into());
        if let Some(props) = value.properties {
            for (key, value) in props.into_iter() {
                let indexed = !value.exclude_from_indexes.unwrap_or(false);
                let meaning = value.meaning.clone();
                result.set(key, value.into(), indexed, meaning);
            }
        }
        result
    }
}

impl Into<google_datastore1::api::Entity> for Entity {
    /// Converts `entail::ds::Entity` into the lower-level API `Entity` by consuming it.
    fn into(self) -> google_datastore1::api::Entity {
        google_datastore1::api::Entity {
            key: Some(self.key.into()),
            properties: Some(
                self.properties
                    .into_iter()
                    .map(|(key, value)| {
                        let indexed = value.indexed;
                        let meaning = value.meaning.clone();
                        let mut val: google_datastore1::api::Value = value.value.into();
                        // Special handling for Array values, where indexing is set on array elements.
                        if let Some(array) = &mut val.array_value {
                            if let Some(values) = &mut array.values {
                                for item in values.iter_mut() {
                                    // The API uses `exclude_from_indexes`, so we negate `indexed`.
                                    item.exclude_from_indexes = Some(!indexed);
                                    item.meaning = meaning;
                                }
                            }
                        } else {
                            // Set indexing flag for non-Array values.
                            val.exclude_from_indexes = Some(!indexed);
                            val.meaning = meaning;
                        }
                        (key.into_owned(), val)
                    })
                    .collect(),
            ),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_building() {
        let key1 = Key::new("Bizz").with_name("buzz");
        assert_eq!(key1.kind(), "Bizz");
        assert_eq!(key1.name(), Some("buzz"));
        assert_eq!(key1.id(), None);
        assert_eq!(key1.parent(), None);
        assert_eq!(key1.to_string(), "Bizz(name:\"buzz\")");
        let key2 = Key::new("Palindrome").with_name("zzub\tbuzz");
        assert_eq!(key2.kind(), "Palindrome");
        assert_eq!(key2.name(), Some("zzub\tbuzz"));
        assert_eq!(key2.id(), None);
        assert_eq!(key2.parent(), None);
        assert_eq!(key2.to_string(), "Palindrome(name:\"zzub\\tbuzz\")");
        let key3 = Key::new("Foo").with_id(123);
        assert_eq!(key3.kind(), "Foo");
        assert_eq!(key3.name(), None);
        assert_eq!(key3.id(), Some(123));
        assert_eq!(key3.parent(), None);
        assert_eq!(key3.to_string(), "Foo(id:123)");
        let key4 = Key::new("Bar")
            .with_name("child")
            .with_parent(Key::new("Foo").with_name("parent"));
        assert_eq!(key4.kind(), "Bar");
        assert_eq!(key4.name(), Some("child"));
        assert_eq!(key4.id(), None);
        assert_ne!(key4.parent(), None);
        if let Some(parent) = key4.parent() {
            assert_eq!(parent.kind(), "Foo");
            assert_eq!(parent.name(), Some("parent"));
            assert_eq!(parent.id(), None);
            assert_eq!(parent.parent(), None);
        }
        assert_eq!(key4.to_string(), "Foo(name:\"parent\")/Bar(name:\"child\")");
    }

    #[test]
    fn test_entity_building() {
        let key = Key::new("Bizz")
            .with_id(1234)
            .with_parent(Key::new("Foo").with_name("parent_name"));
        let mut entity = Entity::new(key);
        entity
            .set_indexed("name", Value::unicode_string("Some Name"))
            .set_unindexed(
                "description",
                Value::unicode_string("A long description that is not indexed."),
            )
            .set_indexed("is_active", Value::boolean(true))
            .set_indexed("score", Value::floating_point(99.9))
            .set_unindexed("data_blob", Value::blob(vec![1, 2, 3, 4, 5]))
            .set_indexed(
                "tags",
                Value::array(vec![
                    Value::UnicodeString("rust".into()),
                    Value::UnicodeString("programming".into()),
                    Value::UnicodeString("datastore".into()),
                ]),
            )
            .set_indexed(
                "related_key",
                Value::key(Key::new("RelatedKind").with_id(5678)),
            );
        println!("{}", entity);

        assert_eq!(entity.key().kind(), "Bizz");
        assert_eq!(entity.key().id(), Some(1234));
        assert_eq!(
            entity.get_value("name").and_then(|v| v.string_value()),
            Some("Some Name")
        );
        assert_eq!(entity.is_indexed("name"), true);
        assert_eq!(entity.is_indexed("description"), false);
        assert_eq!(entity.is_indexed("is_active"), true);
        assert_eq!(entity.is_indexed("score"), true);
        assert_eq!(entity.is_indexed("tags"), true);
        assert_eq!(entity.is_indexed("related_key"), true);
        assert_eq!(entity.is_indexed("non_existent_property"), false);
        let ce: google_datastore1::api::Entity = entity.into();
        assert_eq!(
            ce.properties
                .as_ref()
                .unwrap()
                .get("name")
                .unwrap()
                .exclude_from_indexes,
            Some(false)
        );
        assert_eq!(
            ce.properties
                .as_ref()
                .unwrap()
                .get("tags")
                .unwrap()
                .exclude_from_indexes,
            None
        );
        assert!(
            ce.properties
                .as_ref()
                .unwrap()
                .get("tags")
                .unwrap()
                .array_value
                .as_ref()
                .unwrap()
                .values
                .as_ref()
                .unwrap()
                .iter()
                .all(|item| item.exclude_from_indexes.unwrap() == false)
        );
    }
}
