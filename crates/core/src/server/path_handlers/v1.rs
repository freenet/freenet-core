use super::*;

#[inline]
pub(super) fn get_file_path(uri: axum::http::Uri) -> Result<String, Box<WebSocketApiError>> {
    let p = uri.path().strip_prefix("/v1/contract/").ok_or_else(|| {
        Box::new(WebSocketApiError::InvalidParam {
            error_cause: format!("{uri} not valid"),
        })
    })?;
    let path = p
        .chars()
        .skip_while(|c| ALPHABET.contains(*c))
        .skip_while(|c| c == &'/')
        .skip_while(|c| ALPHABET.contains(*c))
        .skip_while(|c| c == &'/')
        .collect::<String>();
    Ok(path)
}

#[test]
pub(super) fn get_path() {
    let req_path = "/v1/contract/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/web/state.html";
    let base_dir =
        PathBuf::from("/tmp/freenet/webs/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/web/");
    let uri: axum::http::Uri = req_path.parse().unwrap();
    let parsed = get_file_path(uri).unwrap();
    let result = base_dir.join(parsed);
    assert_eq!(
        std::path::PathBuf::from(
            "/tmp/freenet/webs/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/web/state.html"
        ),
        result
    );
}
