use tokio::sync::RwLock;

pub trait BoolToResult<T, E> {
    fn to_result(self, o: T, e: E) -> Result<T, E>;
}
impl<T, E> BoolToResult<T, E> for bool {
    fn to_result(self, o: T, e: E) -> Result<T, E> {
        if self { Ok(o) } else { Err(e) }
    }
}

#[async_trait::async_trait]
pub trait GetCloned<T>
where
    T: Clone,
{
    async fn get_cloned(&self) -> T;
}
#[async_trait::async_trait]
impl<T> GetCloned<T> for RwLock<T>
where
    T: Clone + Send + Sync + std::fmt::Debug,
{
    async fn get_cloned(&self) -> T {
        tracing::debug!("Getting read lock");
        let read_lock = self.read().await;
        tracing::debug!("Got read lock");
        let ret = read_lock.clone();
        drop(read_lock);
        tracing::debug!("Dropped read lock");
        ret
    }
}
