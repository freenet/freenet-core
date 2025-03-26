use super::*;

#[inline]
pub(super) fn get_file_path(uri: axum::http::Uri) -> Result<String, Box<WebSocketApiError>> {
    let p = uri.path().strip_prefix("/v1/contract/").ok_or_else(|| {
        Box::new(WebSocketApiError::InvalidParam {
            error_cause: format!("{uri} not valid"),
        })
    })?;
    
    // Find the second slash which separates the contract ID from the file path
    if let Some(idx) = p.find('/') {
        // Return everything after the first slash
        return Ok(p[idx+1..].to_string());
    }
    
    // If no slash found, return empty string
    Ok(String::new())
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
