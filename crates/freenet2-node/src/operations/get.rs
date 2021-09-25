use std::marker::PhantomData;

/// This is just a placeholder for now!
pub(crate) struct GetOp(PhantomData<()>);

impl GetOp {
    pub fn new() -> Self {
        GetOp(PhantomData)
    }
}
