use std::{convert::TryInto, fs::File, path::PathBuf};

use anyhow::{bail, Result};
use mongodb::{
    bson::{doc, Bson},
    Client,
    Collection,
    Database,
};
use serde_json::Value;

use crate::bench::{Benchmark, COLL_NAME, DATABASE_NAME};

pub struct FindOneBenchmark {
    db: Database,
    num_iter: usize,
    coll: Collection,
}

// Specifies the options to a `FindOneBenchmark::setup` operation.
pub struct Options {
    pub num_iter: usize,
    pub path: PathBuf,
    pub uri: String,
}

#[async_trait::async_trait]
impl Benchmark for FindOneBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        db.drop(None).await?;

        let num_iter = options.num_iter;

        let mut file = spawn_blocking_and_await!(File::open(options.path))?;

        let json: Value = spawn_blocking_and_await!(serde_json::from_reader(&mut file))?;
        let mut doc = match json.try_into()? {
            Bson::Document(doc) => doc,
            _ => bail!("invalid json test file"),
        };

        let coll = db.collection(&COLL_NAME);
        for i in 0..num_iter {
            doc.insert("_id", i as i32);
            coll.insert_one(doc.clone(), None).await?;
        }

        Ok(FindOneBenchmark { db, num_iter, coll })
    }

    async fn do_task(&self) -> Result<()> {
        for i in 0..self.num_iter {
            self.coll
                .find_one(Some(doc! { "_id": i as i32 }), None)
                .await?;
        }

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        self.db.drop(None).await?;

        Ok(())
    }
}