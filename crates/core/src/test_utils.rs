pub fn with_tracing<T>(f: impl FnOnce() -> T) -> T {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_line_number(true)
        .with_file(true)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .finish();
    tracing::subscriber::with_default(subscriber, f)
}
