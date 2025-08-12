pub trait BoolToResult<T, E> {
    fn to_result(self, o: T, e: E) -> Result<T, E>;
}
impl<T, E> BoolToResult<T, E> for bool {
    fn to_result(self, o: T, e: E) -> Result<T, E> {
        if self { Ok(o) } else { Err(e) }
    }
}
