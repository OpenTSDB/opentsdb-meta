use metrics_reporter::MetricsReporter;
#[no_mangle]
pub extern "Rust" fn new() -> Box<dyn MetricsReporter> {
    let metric_reporter_builder = NoopMetricReporterBuilder::new();
    Box::new(metric_reporter_builder.build())
}

#[no_mangle]
pub extern "Rust" fn new_with_ssl(ssl_key: &str, ssl_cert: &str, ca_cert: &str) -> Box<dyn MetricsReporter> {
    let mut metric_reporter_builder = NoopMetricReporterBuilder::new();
    metric_reporter_builder.ssl_key(ssl_key);
    metric_reporter_builder.ssl_cert(ssl_cert);
    metric_reporter_builder.ca_cert(ca_cert);
    Box::new(metric_reporter_builder.build())
}


/// A Metric Reporter plugin that just prints the metric values when set.
/// Created as an example, but can be used as default reporter
/// if one doesn't want to push metrics to any monitoring system.
/// build this with cargo build --release and copy the dll from target
/// to the plugin path in config of your binary.
pub struct NoopMetricReporterBuilder {
    pub ssl_key: Option<str>,
    pub ssl_cert: Option<str>,
    pub ca_cert: Option<str>,
}

impl NoopMetricReporterBuilder {
    fn new() -> NoopMetricReporterBuilder {
        NoopMetricReporterBuilder {
            ssl_key: None,
            ssl_cert: None,
            ca_cert: None,
        }
    }

    fn ssl_key(mut self, key: &str) -> Self {
        self.ssl_key = Some(*key);
        self
    }

    fn ssl_cert(mut self, cert: &str) -> Self {
        self.ssl_cert = Some(*cert);
        self
    }

    fn ca_cert(mut self, cert: &str) -> Self {
        self.ssl_key = Some(*key);
        self
    }
}

struct NoopMetricReporter;

impl MetricReporter for NoopMetricReporter {

    fn build(mut self) -> NoopMetricReporter {
        self
    }

    fn count(&mut self, metric: &str, tags: &[&str], value: u64) {
        println!("Incrementing counter for metric {} for tags {:?} with value {}", metric, tags, value);
    }

    fn gauge(&mut self, metric: &str, tags: &[&str], value: u64) {
        println!("Setting gauge for metric {} for tags {:?} with value {}", metric, tags, value);
    }
}