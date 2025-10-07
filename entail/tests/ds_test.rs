use entail::{
    EntailError,
    ds::{DatastoreShell, Entity, Key, Mutation, MutationBatch, Transaction, Value},
};

#[tokio::test]
pub async fn test_create_conflict() {
    let result = create_conflict().await;
    match result {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{:?}", err);
        }
    }
}

pub async fn create_conflict() -> Result<(), EntailError> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap();

    let ds = DatastoreShell::new("test-project", false, None)
        .await
        .map_err(|_| Default::default())?;
    let key = Key::new("Test").with_name("one");
    ds.commit(MutationBatch::new().add(Mutation::Upsert({
        let mut e = Entity::new(key.clone());
        e.set_indexed("glank", Value::unicode_string("cluff"));
        e
    })))
    .await?;
    let t1 = Transaction::new(&ds).run(|ts| {
        let key1 = key.clone();
        Box::pin(async move {
            eprintln!("a1..");
            _ = ts.get_single(key1.clone()).await?;
            eprintln!("a2..");
            ts.commit(MutationBatch::new().add(Mutation::Upsert({
                let mut e = Entity::new(key1.clone());
                e.set_indexed("glank", Value::unicode_string("wibz"));
                e
            })))
            .await?;
            eprintln!("a3..");
            Ok(())
        })
    });
    let t2 = Transaction::new(&ds).run(|ts| {
        let key2 = key.clone();
        Box::pin(async move {
            eprintln!("b1..");
            _ = ts.get_single(key2.clone()).await?;
            eprintln!("b2..");
            ts.commit(MutationBatch::new().add(Mutation::Upsert({
                let mut e = Entity::new(key2.clone());
                e.set_indexed("glank", Value::unicode_string("vrob"));
                e
            })))
            .await?;
            eprintln!("b3..");
            Ok(())
        })
    });
    let (a, b) = tokio::join!(t1, t2);
    a?;
    b?;
    Ok(())
}
