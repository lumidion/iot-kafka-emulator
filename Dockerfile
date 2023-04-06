FROM rust

COPY target/debug/iot-kafka-emulator /bin/iot-kafka-emulator
CMD ["/bin/iot-kafka-emulator"]