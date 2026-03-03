use std::ffi::CString;
use std::sync::{Mutex, MutexGuard};

pub(crate) fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(crate) fn to_lossy_cstring(value: &str) -> CString {
    let mut bytes = value.as_bytes().to_vec();
    bytes.retain(|b| *b != 0);
    CString::new(bytes).unwrap_or_default()
}
