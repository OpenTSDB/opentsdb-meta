FROM rust:1.51 AS build
RUN apt-get update \
    && apt-get -y install clang
WORKDIR ./dep-download
COPY . .
RUN rm ./Cargo.lock \
    && rm -rf ./target
RUN rustup override set stable \
    && rustup default stable \
    && rustup component add rustfmt
RUN cargo build --release

FROM debian:buster-slim
RUN apt-get update \
    && apt-get -y install clang openssl
RUN mkdir -p /etc/myst          \
    && mkdir -p /var/log/myst   \
    && mkdir -p /var/myst/data  \
    && mkdir -p /var/myst/tmp
RUN  ln -s /usr/lib/x86_64-linux-gnu/libssl.so.1.1 /usr/local/lib64/libssl.so.1.1
COPY --from=build /dep-download/target/release/server /usr/bin/myst
COPY --from=build /dep-download/run_script.sh /usr/bin/run_script.sh
EXPOSE 9999
CMD ["/bin/sh", "-c", "/usr/bin/run_script.sh"]
