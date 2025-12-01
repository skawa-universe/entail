use entail::{Entail, EntailErrorKind, EntityModel, ds};
use std::collections::HashSet;

#[derive(Entail, Debug, Default)]
#[entail(rename_all = "camelCase")]
struct Model {
    #[entail(key)]
    name: String,
    #[entail()]
    some_field: String,
    #[entail(field, unindexed)]
    key: Option<i32>,
    #[entail]
    lookup: Vec<String>,
    #[entail(unindexed)]
    bin: Vec<u8>,
    #[entail]
    related: Option<ds::Key>,
    #[entail]
    some_bool: bool,
    #[entail(text)]
    opt_text: Option<String>,
    #[entail(text)]
    present_text: String,
    #[entail]
    nullable_blob: Option<Vec<u8>>,
    #[entail]
    opt_blob: Option<Vec<u8>>,
    unrelated: Option<HashSet<String>>,
}

#[derive(Entail, Debug)]
#[entail(rename_all = "camelCase", name = "MM")]
struct MinimalModel {
    #[entail]
    key: ds::Key,
    #[entail(text)]
    text_field: String,
}

impl Default for MinimalModel {
    fn default() -> Self {
        Self {
            key: Self::adapter().create_key().with_name("juff"),
            text_field: String::default(),
        }
    }
}

#[derive(Entail, Debug, Default)]
#[entail]
struct AutoId {
    #[entail]
    key: Option<i64>,
    #[entail(unindexed_nulls)]
    value: Option<i64>,
}

#[derive(Entail, Debug, Default)]
#[entail]
struct ManualId {
    #[entail]
    key: i64,
    #[entail(unindexed_nulls)]
    value: Option<i64>,
}

#[test]
fn code_gen() {
    let model = Model {
        name: "foo".into(),
        some_field: "bar".into(),
        key: Some(118999),
        lookup: vec![
            String::from("wow"),
            String::from("such"),
            String::from("index"),
        ],
        bin: vec![1, 2, 3],
        unrelated: Some(HashSet::new()),
        some_bool: true,
        present_text: "present text".into(),
        nullable_blob: None,
        opt_blob: Some(vec![4, 5, 6, 7]),
        ..Model::default()
    };
    let mut e = model.to_ds_entity().unwrap();
    println!("{}", e);
    assert!(e.is_indexed("someField"));
    assert!(!e.is_indexed("key"));
    assert!(e.is_indexed("lookup"));
    assert!(e.is_indexed("related"));
    assert_eq!(
        e.get_value("someField"),
        Some(ds::Value::unicode_string("bar")).as_ref()
    );
    assert_eq!(
        e.get_value("key"),
        Some(ds::Value::integer(118999)).as_ref()
    );
    assert_eq!(
        e.get_value("lookup"),
        Some(ds::Value::array(vec![
            ds::Value::unicode_string("wow"),
            ds::Value::unicode_string("such"),
            ds::Value::unicode_string("index"),
        ]))
        .as_ref()
    );
    assert_eq!(
        e.get_value("bin"),
        Some(ds::Value::blob(vec![1, 2, 3])).as_ref()
    );
    assert!(e.is_indexed("nullableBlob"));
    assert_eq!(e.get_value("nullableBlob"), Some(ds::Value::Null).as_ref());
    assert!(e.is_indexed("optBlob"));
    assert_eq!(
        e.get_value("optBlob"),
        Some(ds::Value::blob(vec![4, 5, 6, 7])).as_ref()
    );
    assert_eq!(
        e.get_value("someBool"),
        Some(ds::Value::boolean(true)).as_ref()
    );
    assert_eq!(e.get_value("related"), Some(ds::Value::null()).as_ref());
    let raw_entity: google_datastore1::api::Entity = e.clone().into();
    let null_text_field = raw_entity
        .properties
        .as_ref()
        .unwrap()
        .get("optText")
        .unwrap();
    assert_eq!(null_text_field.meaning, None);
    assert_eq!(null_text_field.exclude_from_indexes, Some(false));
    let present_text_field = raw_entity
        .properties
        .as_ref()
        .unwrap()
        .get("presentText")
        .unwrap();
    assert_eq!(present_text_field.meaning, Some(ds::MEANING_TEXT));
    assert_eq!(present_text_field.exclude_from_indexes, Some(true));
    let related_key = ds::Key::new("Bizz").with_name("buzz");
    e.set_indexed("related", ds::Value::key(related_key.clone()));
    let new_model = Model::from_ds_entity(&e).expect("Cannot create from entity");
    assert_eq!(new_model.name, model.name);
    assert_eq!(new_model.some_field, model.some_field);
    assert_eq!(new_model.key, model.key);
    assert_eq!(new_model.lookup, model.lookup);
    assert!(new_model.some_bool);
    assert_eq!(new_model.related.as_ref(), Some(&related_key));
    assert!(new_model.unrelated.is_none());
    assert_eq!(new_model.nullable_blob, None);
    assert_eq!(new_model.opt_blob, Some(vec![4, 5, 6, 7]));
    println!("{:?}", new_model);
}

#[test]
fn code_gen_minimal_model() {
    let min_mod = MinimalModel {
        key: MinimalModel::adapter().create_named_key("wibz"),
        text_field: "foo".into(),
    };
    let e = min_mod.to_ds_entity().unwrap();
    assert_eq!(&ds::Key::new("MM").with_name("wibz"), e.key());
    let field = e.get("textField").unwrap();
    assert_eq!(field.meaning().unwrap(), ds::MEANING_TEXT);
    let different_kind = ds::Entity::new(ds::Key::new("NotMM").with_id(1));
    let result = MinimalModel::from_ds_entity(&different_kind).expect_err("Expected an error");
    assert_eq!(result.kind, EntailErrorKind::EntityKindMismatch);
    println!("Expected error message: {}", result.message);
}

#[test]
fn code_gen_auto_id() {
    let auto_id = AutoId::default();
    let mut e = auto_id.to_ds_entity().unwrap();
    let key = e.key();
    assert_eq!(e.kind(), "AutoId");
    assert!(!key.is_complete());
    let id: i64 = 1189998899991197253;
    e.set_key(key.clone().with_id(id));
    let auto_with_id = AutoId::from_ds_entity(&e).unwrap();
    assert!(auto_with_id.key.is_some());
    assert_eq!(auto_with_id.key.unwrap(), id);

    let manual_id = ManualId {
        key: 1234i64,
        ..ManualId::default()
    };
    let mut e = manual_id.to_ds_entity().unwrap();
    assert_eq!(e.kind(), "ManualId");
    assert!(e.key().is_complete());
    assert_eq!(e.key().id(), Some(1234i64));
    e.set_key(ManualId::adapter().create_key());
    ManualId::from_ds_entity(&e)
        .expect_err("Should have returned an error since the key is incomplete");
}
