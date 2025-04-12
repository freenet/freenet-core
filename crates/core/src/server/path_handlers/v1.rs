use super::*;

#[inline]
pub(super) fn get_file_path(uri: axum::http::Uri) -> Result<String, Box<WebSocketApiError>> {
    let path_str = uri.path();

    let remainder = if let Some(rem) = path_str.strip_prefix("/v1/contract/web/") {
        // Handle the specific web asset route: /v1/contract/web/{key}/{path}
        rem // remainder is "{key}/{path}"
    } else if let Some(rem) = path_str.strip_prefix("/v1/contract/") {
        // Handle the general contract route: /v1/contract/{key}/{path}
        rem // remainder is "{key}/{path}" or just "{key}"
    } else {
        // Path doesn't match expected prefixes
        return Err(Box::new(WebSocketApiError::InvalidParam {
            error_cause: format!(
                "URI path '{path_str}' does not start with /v1/contract/ or /v1/contract/web/"
            ),
        }));
    };

    // Now, remainder contains "{key}/{path}" or just "{key}"
    // Find the first '/' which separates the key from the actual path.
    let file_path = match remainder.split_once('/') {
        // If a '/' exists, everything after it is the path.
        Some((_key, path)) => path.to_string(),
        // If no '/' exists after the key (e.g., URL was just "/v1/contract/CONTRACT_KEY" or "/v1/contract/web/CONTRACT_KEY"),
        // it implies the root. An empty path string "" is appropriate here,
        // which will likely be handled by serving index.html later.
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
