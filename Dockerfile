FROM rust:1.68 as build

RUN USER=root cargo new --bin iot-kafka-emulator
WORKDIR /iot-kafka-emulator

COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

RUN cargo build --release
RUN rm ./src/*.rs

COPY ./src ./src

RUN rm ./target/release/deps/iot_kafka_emulator*

RUN cargo build --release

FROM debian:buster-slim

COPY --from=build /iot-kafka-emulator/target/release/iot-kafka-emulator .

CMD ["./iot-kafka-emulator"]