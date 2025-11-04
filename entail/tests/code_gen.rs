use entail::{Entail, EntityModel, ds};
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
    assert_eq!(e.get_value("key"), Some(ds::Value::integer(118999)).as_ref());
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
        Some(ds::Value::Blob(vec![1, 2, 3].into())).as_ref()
    );
    assert_eq!(e.get_value("someBool"), Some(ds::Value::boolean(true)).as_ref());
    assert_eq!(e.get_value("related"), Some(ds::Value::null()).as_ref());
    let raw_entity: google_datastore1::api::Entity = e.clone().into();
    let null_text_field = raw_entity.properties.as_ref().unwrap().get("optText").unwrap();
    assert_eq!(null_text_field.meaning, None);
    assert_eq!(null_text_field.exclude_from_indexes, Some(false));
    let present_text_field = raw_entity.properties.as_ref().unwrap().get("presentText").unwrap();
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
    let field = e.get("opt").unwrap();
    assert_eq!(field.meaning(), None);
}
