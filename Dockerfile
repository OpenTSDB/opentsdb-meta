FROM rust:1.51 AS build
RUN apt-get update \
    && apt-get -y install clang
WORKDIR ./dep-download
COPY . .
RUN rm -f ./Cargo.lock \
    && rm -rf ./target
RUN rustup override set stable \
    && rustup default stable \
    && rustup component add rustfmt
RUN cargo build --release

FROM debian:buster-slim
RUN apt-get update \
    && apt-get -y install clang ca-certificates
RUN mkdir -p /usr/share/myst/bin     \
    && mkdir -p /usr/share/myst/plugins  \
    && mkdir -p /var/log/myst        \
    && mkdir -p /var/myst/data       \
    && mkdir -p /var/myst/tmp
COPY --from=build /dep-download/target/release/libnoop_metrics_reporter.so /usr/share/myst/plugins/metrics-reporter
COPY --from=build /dep-download/target/release/server /usr/share/myst/bin/myst-server
COPY --from=build /dep-download/target/release/segment-gen /usr/share/myst/bin/myst-segment-gen

COPY --from=build /dep-download/run_script.sh /usr/bin/run_script.sh
EXPOSE 9999
ENTRYPOINT ["/bin/bash", "/usr/bin/run_script.sh"]
