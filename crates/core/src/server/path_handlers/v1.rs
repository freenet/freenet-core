use super::*;

#[inline]
pub(super) fn get_file_path(uri: axum::http::Uri) -> Result<String, Box<WebSocketApiError>> {
    let path_str = uri.path();

    let remainder = if let Some(rem) = path_str.strip_prefix("/v1/contract/web/") {
        rem
    } else if let Some(rem) = path_str.strip_prefix("/v1/contract/") {
        rem
    } else if let Some(rem) = path_str.strip_prefix("/v2/contract/web/") {
        rem
    } else if let Some(rem) = path_str.strip_prefix("/v2/contract/") {
        rem
    } else {
        return Err(Box::new(WebSocketApiError::InvalidParam {
            error_cause: format!(
                "URI path '{path_str}' does not start with /v1/contract/ or /v2/contract/"
            ),
        }));
    };

    // Now, remainder contains "{key}/{path}" or just "{key}"
    // Find the first '/' which separates the key from the actual path.
    let file_path = match remainder.split_once('/') {
        Some((_key, path)) => path.to_string(),
        None => "".to_string(),
    };

    Ok(file_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_path_v1() {
        let req_path = "/v1/contract/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/state.html";
        let base_dir = PathBuf::from(
            "/tmp/freenet/webapp_cache/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/",
        );
        let uri: axum::http::Uri = req_path.parse().unwrap();
        let parsed = get_file_path(uri).unwrap();
        let result = base_dir.join(parsed);
        assert_eq!(
            PathBuf::from(
                "/tmp/freenet/webapp_cache/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/state.html"
            ),
            result
        );
    }

    #[test]
    fn get_path_v2() {
        let req_path = "/v2/contract/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/state.html";
        let base_dir = PathBuf::from(
            "/tmp/freenet/webapp_cache/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/",
        );
        let uri: axum::http::Uri = req_path.parse().unwrap();
        let parsed = get_file_path(uri).unwrap();
        let result = base_dir.join(parsed);
        assert_eq!(
            PathBuf::from(
                "/tmp/freenet/webapp_cache/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/state.html"
            ),
            result
        );
    }

    #[test]
    fn get_path_v2_web() {
        let req_path =
            "/v2/contract/web/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/assets/app.js";
        let uri: axum::http::Uri = req_path.parse().unwrap();
        let parsed = get_file_path(uri).unwrap();
        assert_eq!(parsed, "assets/app.js");
    }
}
