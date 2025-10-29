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
    unrelated: Option<HashSet<String>>,
}

#[derive(Entail, Debug)]
#[entail(rename_all = "camelCase")]
struct MinimalModel {
    #[entail]
    key: ds::Key,
}

impl Default for MinimalModel {
    fn default() -> Self {
        Self {
            key: Self::adapter().create_key().with_name("juff"),
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
        ..Model::default()
    };
    let mut e = model.to_ds_entity().unwrap();
    println!("{}", e);
    assert!(e.is_indexed("someField"));
    assert!(!e.is_indexed("key"));
    assert!(e.is_indexed("lookup"));
    assert!(e.is_indexed("related"));
    assert_eq!(
        e.get("someField"),
        Some(ds::Value::unicode_string("bar")).as_ref()
    );
    assert_eq!(e.get("key"), Some(ds::Value::integer(118999)).as_ref());
    assert_eq!(
        e.get("lookup"),
        Some(ds::Value::array(vec![
            ds::Value::unicode_string("wow"),
            ds::Value::unicode_string("such"),
            ds::Value::unicode_string("index"),
        ]))
        .as_ref()
    );
    assert_eq!(
        e.get("bin"),
        Some(ds::Value::Blob(vec![1, 2, 3].into())).as_ref()
    );
    assert_eq!(
        e.get("someBool"),
        Some(ds::Value::boolean(true)).as_ref()
    );
    assert_eq!(e.get("related"), Some(ds::Value::null()).as_ref());
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
        key: ds::Key::new("MinimalModel").with_name("wibz"),
    };
    let e = min_mod.to_ds_entity().unwrap();
    assert_eq!(&ds::Key::new("MinimalModel").with_name("wibz"), e.key());
}
