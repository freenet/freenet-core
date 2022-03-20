use crate::Config;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::sdk::trace::{IdGenerator, Sampler, Tracer};
use opentelemetry::sdk::trace;
use opentelemetry::{global, trace::TraceError, Context, KeyValue};
use tracing_subscriber::layer::SubscriberExt;

pub fn init_tracer(service_name: String) -> Result<(), TraceError> {
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name(service_name)
        .with_trace_config(
            trace::config()
                .with_sampler(Sampler::AlwaysOn)
                .with_id_generator(IdGenerator::default()),
        )
        //.with_collector_endpoint("127.0.0.1:6831")
        .install_simple()
        .unwrap();

    global::set_text_map_propagator(TraceContextPropagator::new());
    //global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(tracing_subscriber::fmt::layer());

    tracing::subscriber::set_global_default(subscriber).expect("Error setting subscriber");

    Ok(())
}