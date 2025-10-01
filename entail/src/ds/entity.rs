use super::super::*;
use std::collections::HashMap;

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum KeyVariant {
    Name(Cow<'static, str>),
    Id(i64),
    Incomplete,
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub struct Key {
    kind: Cow<'static, str>,
    variant: KeyVariant,
    parent: Option<Box<Key>>,
}

impl Key {
    pub fn new(kind: impl Into<Cow<'static, str>>) -> Self {
        Key {
            kind: kind.into(),
            variant: KeyVariant::Incomplete,
            parent: None,
        }
    }

    pub fn kind(&self) -> &str {
        self.kind.as_ref()
    }

    pub fn name(&self) -> Option<&str> {
        if let KeyVariant::Name(name) = &self.variant {
            Some(name.as_ref())
        } else {
            None
        }
    }

    pub fn id(&self) -> Option<i64> {
        if let KeyVariant::Id(id) = &self.variant {
            Some(*id)
        } else {
            None
        }
    }

    pub fn parent(&self) -> Option<&Key> {
        self.parent.as_deref()
    }

    pub fn with_name(self, name: impl Into<Cow<'static, str>>) -> Self {
        Key {
            variant: KeyVariant::Name(name.into()),
            ..self
        }
    }

    pub fn with_id(self, id: i64) -> Self {
        Key {
            variant: KeyVariant::Id(id),
            ..self
        }
    }

    pub fn with_parent(self, parent: Key) -> Self {
        Key {
            parent: Some(Box::new(parent)),
            ..self
        }
    }

    pub fn with_no_parent(self) -> Self {
        Key {
            parent: None,
            ..self
        }
    }

    pub fn with_boxed_parent(self, parent: Option<Box<Key>>) -> Self {
        Key {
            parent: parent,
            ..self
        }
    }

    fn push_path_elements(self, out: &mut Vec<google_datastore1::api::PathElement>) {
        if let Some(parent) = self.parent {
            parent.push_path_elements(out);
        }
        out.push(match self.variant {
            KeyVariant::Id(id) => google_datastore1::api::PathElement {
                kind: Some(self.kind.to_string()),
                id: Some(id),
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
}

impl Into<google_datastore1::api::Key> for Key {
    fn into(self) -> google_datastore1::api::Key {
        let mut path = Vec::new();
        self.push_path_elements(&mut path);
        google_datastore1::api::Key {
            partition_id: None,
            path: Some(path),
        }
    }
}

impl From<google_datastore1::api::Entity> for Entity {
    fn from(value: google_datastore1::api::Entity) -> Entity {
        let mut result = Entity::new(value.key.expect("Missing key").into());
        if let Some(props) = value.properties {
            for (key, value) in props.into_iter() {
                let indexed = value.exclude_from_indexes.unwrap_or(false);
                result.set(key, value.into(), indexed);
            }
        }
        result
    }
}

impl From<google_datastore1::api::Key> for Key {
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

#[derive(PartialEq, Debug)]
pub enum Value {
    Null,
    Integer(i64),
    Boolean(bool),
    Blob(Cow<'static, [u8]>),
    UnicodeString(Cow<'static, str>),
    FloatingPoint(f64),
    Array(Vec<Value>),
    Key(Key),
}

impl Value {
    pub fn null() -> Value {
        Value::Null
    }

    pub fn integer(val: i64) -> Value {
        Value::Integer(val)
    }

    pub fn boolean(val: bool) -> Value {
        Value::Boolean(val)
    }

    pub fn blob(val: impl Into<Cow<'static, [u8]>>) -> Value {
        Value::Blob(val.into())
    }

    pub fn unicode_string(s: impl Into<Cow<'static, str>>) -> Value {
        Value::UnicodeString(s.into())
    }

    pub fn floating_point(val: f64) -> Value {
        Value::FloatingPoint(val)
    }

    pub fn array(val: Vec<Value>) -> Value {
        Value::Array(val)
    }

    pub fn key(key: Key) -> Value {
        Value::Key(key)
    }

    pub fn string_value(&self) -> Option<&str> {
        match self {
            Self::UnicodeString(str) => Some(str.as_ref()),
            _ => None,
        }
    }

    pub fn key_value(&self) -> Option<&Key> {
        match self {
            Self::Key(key) => Some(key),
            _ => None,
        }
    }

    pub fn blob_value(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(bytes) => Some(bytes),
            _ => None,
        }
    }
}

impl Into<Value> for google_datastore1::api::Value {
    fn into(self) -> Value {
        if let Some(integer_value) = self.integer_value {
            Value::Integer(integer_value)
        } else if let Some(boolean_value) = self.boolean_value {
            Value::Boolean(boolean_value)
        } else if let Some(blob_value) = self.blob_value {
            Value::Blob(Cow::Owned(blob_value))
        } else if let Some(string_value) = self.string_value {
            Value::UnicodeString(Cow::Owned(string_value))
        } else if let Some(double_value) = self.double_value {
            Value::FloatingPoint(double_value)
        } else if let Some(array_value) = self.array_value {
            let values: Vec<Value> = array_value
                .values
                .unwrap_or_default()
                .into_iter()
                .map(|e| e.into())
                .collect();
            Value::Array(values)
        } else if let Some(key_value) = self.key_value {
            Value::Key(key_value.into())
        } else if self.entity_value.is_some() || self.geo_point_value.is_some() || self.timestamp_value.is_some() {
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

impl fmt::Display for Value {
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

#[derive(PartialEq, Debug)]
struct PropertyValue {
    value: Value,
    indexed: bool,
}

impl fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.value.fmt(f)?;
        if self.indexed {
            write!(f, " (indexed)")
        } else {
            write!(f, " (unindexed)")
        }
    }
}

#[derive(Debug)]
pub struct Entity {
    key: Key,
    properties: HashMap<Cow<'static, str>, PropertyValue>,
}

impl Entity {
    pub fn new(key: Key) -> Self {
        Self {
            key,
            properties: HashMap::new(),
        }
    }

    pub fn of_kind(kind: impl Into<Cow<'static, str>>) -> Self {
        Self {
            key: Key::new(kind),
            properties: HashMap::new(),
        }
    }

    pub fn key(&self) -> &Key {
        &self.key
    }

    pub fn set_key(&mut self, key: Key) -> &mut Self {
        self.key = key;
        self
    }

    pub fn set(
        &mut self,
        name: impl Into<Cow<'static, str>>,
        value: Value,
        indexed: bool,
    ) -> &mut Self {
        self.properties
            .insert(name.into(), PropertyValue { value, indexed });
        self
    }

    pub fn set_unindexed(&mut self, name: impl Into<Cow<'static, str>>, value: Value) -> &mut Self {
        self.set(name, value, false)
    }

    pub fn set_indexed(&mut self, name: impl Into<Cow<'static, str>>, value: Value) -> &mut Self {
        self.set(name, value, true)
    }

    pub fn is_indexed(&self, name: &str) -> bool {
        self.properties
            .get(name)
            .map(|v| v.indexed)
            .unwrap_or(false)
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.properties.get(name).map(|ev| &ev.value)
    }
}

impl fmt::Display for Entity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.key.fmt(f)?;
        write!(f, " {{")?;
        for (key, value) in self.properties.iter() {
            write!(f, "\n  {}: {},", key, value)?;
        }
        write!(f, "\n}}")
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
            entity.get("name").and_then(|v| v.string_value()),
            Some("Some Name")
        );
        assert_eq!(entity.is_indexed("name"), true);
        assert_eq!(entity.is_indexed("description"), false);
        assert_eq!(entity.is_indexed("is_active"), true);
        assert_eq!(entity.is_indexed("score"), true);
        assert_eq!(entity.is_indexed("tags"), true);
        assert_eq!(entity.is_indexed("related_key"), true);
        assert_eq!(entity.is_indexed("non_existent_property"), false);
    }
}
