FROM rust:latest AS builder

# COPY --exclude=target . /code
ADD . /code
WORKDIR /code
ENV HTTPS_PROXY=http://192.168.0.102:8080
RUN cargo build


FROM debian:bookworm

# ADD ./target/x86_64-unknown-linux-musl/debug/cdl /cdl
COPY --from=builder /code/target/debug/cdl /cdl

RUN adduser --disabled-password test

ENV RUST_LOG=INFO
