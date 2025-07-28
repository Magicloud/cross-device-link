FROM alpine:latest

ADD ./target/x86_64-unknown-linux-musl/debug/cdl /cdl

RUN adduser -D test

# ENV RUST_LOG=DEBUG