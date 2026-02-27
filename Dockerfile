# Single-Stage Build (Larger Image)
FROM rust:latest AS builder

# Set the working directory inside the container
WORKDIR /usr/src/app

# Install protoc (protobuf-compiler)
RUN apt-get update && apt-get install -y protobuf-compiler

# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.toml Cargo.lock ./

# Copy the source code
COPY src ./src
COPY proto ./proto
COPY build.rs ./
COPY words.txt ./
COPY words_alpha.txt ./

# Binary to build/run
ARG APP_BIN=ftrie-server
ENV APP_BIN=${APP_BIN}

# Build the application with the specified binary target
RUN cargo build --release --bin ${APP_BIN}

# Expose the server's default gRPC port
EXPOSE 50051
ENV BIND_ADDR=0.0.0.0:50051

# Run the application
CMD ["sh", "-c", "./target/release/${APP_BIN}"]
