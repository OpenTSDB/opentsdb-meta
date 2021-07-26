OpenTSDB Metadata Store
===============================
[![Build Status][status-image]][status-url]

Myst is OpenTSDB's persistent Metadata Store. Myst has the capability to read raw timeseries Metadata from any remote storage system and also flush segments to any remote storage system.  

Configuration
-------------

Myst reads configuration from `/etc/myst/myst.toml`.
Please see `src/utils/config.rs` for the required configurations.

Usage
-----

### Using Cargo
#### 1. Run Server
```cargo run --bin server --release```
#### 2. Run Segment Generator
```cargo run --bin segment-gen --release```

### Using Binary
#### 1. Build using cargo
```cargo build --release```
#### 2. Run Server
```sh target/release/server```
#### 3. Run Segment Generator
```sh target/release/segment-gen```

### Using Docker:
#### 1. Build:
```docker build . -t opentsdb-meta```
#### 2. Run Server
```docker run --name opentsdb-meta IMAGE_ID server```
#### 3. Run Segment Generator
```docker run --name opentsdb-meta IMAGE_ID segment-gen```

Plugins
-----
#### Instrumentation

[metrics-reporter](metrics-reporter) contains the traits used to report counter and gauge metrics. 
The binaries (server and segment-gen) load plugins during runtime that implement this trait. 

A sample plugin [noop-metrics-reporter](noop-metrics-reporter) is provided for your reference. Building this will generate a shared library (`*so` or `*dll`) based on the platform. 
[Server](myst/src/server/server.rs) loads this shared library during run time from the `plugin_path` specified in the config file and reports the metrics. 



Contribute
----------

Please see the [Contributing](Contributing.md) file for information on how to
get involved. We welcome issues, questions, and pull requests.

Maintainers
-----------

* Siddartha Guthikonda
* Ravi Kiran Chiruvolu

License
-------

This project is licensed under the terms of the Apache 2.0 open source license.
Please refer to [LICENSE](LICENSE.md) for the full terms.

[status-image]: https://cd.screwdriver.cd/pipelines/7648/badge
[status-url]: https://cd.screwdriver.cd/pipelines/7648
