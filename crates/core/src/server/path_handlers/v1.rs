use super::*;

#[inline]
pub(super) fn get_file_path(uri: axum::http::Uri) -> Result<String, Box<WebSocketApiError>> {
    let p = uri.path().strip_prefix("/v1/contract/").ok_or_else(|| {
        Box::new(WebSocketApiError::InvalidParam {
            error_cause: format!("{uri} not valid"),
        })
    })?;
    // p now contains something like "CONTRACT_KEY/path/to/file.html" or "CONTRACT_KEY/"

    // Find the first '/' which separates the key from the actual path.
    let file_path = match p.split_once('/') {
        // If a '/' exists, everything after it is the path.
        Some((_key, path)) => path.to_string(),
        // If no '/' exists after the key (e.g., URL was just "/v1/contract/CONTRACT_KEY"),
        // it implies the root. An empty path string is appropriate here,
        // potentially mapping to index.html later in the logic.
        None => "".to_string(),
    };

    Ok(file_path)
}

#[test]
pub(super) fn get_path() {
    let req_path = "/v1/contract/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/state.html";
    let base_dir =
        PathBuf::from("/tmp/freenet/webapp_cache/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/");
    let uri: axum::http::Uri = req_path.parse().unwrap();
    let parsed = get_file_path(uri).unwrap();
    let result = base_dir.join(parsed);
    assert_eq!(
        std::path::PathBuf::from(
            "/tmp/freenet/webapp_cache/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/state.html"
        ),
        result
    );
}
