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
RUN mkdir -p /etc/myst          \
    && mkdir -p /var/log/myst   \
    && mkdir -p /var/myst/data  \
    && mkdir -p /var/myst/tmp
COPY --from=build /dep-download/target/release/server /usr/bin/myst-server
COPY --from=build /dep-download/target/release/segment-gen /usr/bin/myst-segment-gen

COPY --from=build /dep-download/run_script.sh /usr/bin/run_script.sh
EXPOSE 9999
ENTRYPOINT ["/bin/bash", "/usr/bin/run_script.sh"]
