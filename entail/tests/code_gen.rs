use entail::{ds, Entail, EntityModel};

//*

#[derive(Entail, Debug)]
#[entail(rename_all="camelCase")]
struct Model {
  #[entail(key)]
  name: String,
  #[entail()]
  some_field: String,
  #[entail(field, unindexed)]
  key: Option<i32>,
  #[entail]
  lookup: Vec<String>,
  #[entail]
  related: Option<ds::Key>,
}

/*/

#[derive(Debug)]
struct Model {
  name: String,
  some_field: String,
  key: Option<i32>,
  lookup: Vec<String>,
  related: Option<ds::Key>,
}

// */

#[test]
fn code_gen() {
  let model = Model {
    name: "foo".into(),
    some_field: "bar".into(),
    key: Some(118999),
    lookup: vec![String::from("wow"), String::from("such"), String::from("index")],
    related: None,
  };
  let e = model.to_ds_entity().unwrap();
  println!("{}", e);
  assert!(e.is_indexed("someField"));
  assert!(!e.is_indexed("key"));
  assert!(e.is_indexed("lookup"));
  assert!(e.is_indexed("related"));
  assert_eq!(e.get("someField"), Some(ds::Value::unicode_string("bar")).as_ref());
  assert_eq!(e.get("key"), Some(ds::Value::integer(118999)).as_ref());
  assert_eq!(e.get("lookup"), Some(ds::Value::array(vec![
    ds::Value::unicode_string("wow"),
    ds::Value::unicode_string("such"),
    ds::Value::unicode_string("index"),
  ])).as_ref());
  assert_eq!(e.get("related"), Some(ds::Value::null()).as_ref());
  let new_model = Model::from_ds_entity(&e);
  println!("{:?}", new_model);
}
