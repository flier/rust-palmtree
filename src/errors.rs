use std::result::Result as StdResult;

use failure::Error;

pub type Result<T> = StdResult<T, Error>;

#[derive(Debug, Fail)]
pub enum PalmTreeError {
    #[fail(display = "too many strong references: {}", _0)]
    UnwrapRC(usize),

    #[fail(display = "fail to stop worker-{}", _0)]
    StopWorker(usize),
}
