// mod atlas_connectivity;
// mod client;
// mod coll;
// mod db;
// mod spec;
mod util;

// pub(crate) use self::util::{assert_matches, parse_version, CommandEvent, EventClient, Matchable};

use lazy_static::lazy_static;

use self::util::{TestClient, TestLock};
use crate::{feature::AsyncRuntime, options::ClientOptions};

const MAX_POOL_SIZE: u32 = 100;

lazy_static! {
    pub(crate) static ref ASYNC_RUNTIME: AsyncRuntime = Default::default();
    pub(crate) static ref CLIENT: TestClient = ASYNC_RUNTIME.block_on(TestClient::new());
    pub(crate) static ref CLIENT_OPTIONS: ClientOptions = {
        let uri = option_env!("MONGODB_URI").unwrap_or("mongodb://localhost:27017");
        let mut options = ClientOptions::parse(uri).unwrap();
        options.max_pool_size = Some(MAX_POOL_SIZE);

        options
    };
    pub(crate) static ref LOCK: TestLock = TestLock::new();
}
