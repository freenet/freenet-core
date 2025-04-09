use super::*;

#[inline]
pub(super) fn get_file_path(uri: axum::http::Uri) -> Result<String, Box<WebSocketApiError>> {
    let p = uri.path().strip_prefix("/v1/contract/").ok_or_else(|| {
        Box::new(WebSocketApiError::InvalidParam {
            error_cause: format!("{uri} not valid"),
        })
    })?;
    // Find the position of the first '/' which separates the contract key from the file path.
    // Everything after this slash is considered the file path within the contract.
    let file_path_part = p.find('/').map(|idx| &p[idx + 1..]).unwrap_or("");
    Ok(file_path_part.to_string())
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
