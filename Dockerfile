# Shopping Sessions entry: native Rust pipeline, standard library only (no crates).
# BASE and RUNTIME can be overridden for local builds behind a registry mirror.
ARG BASE=rust:1.85-slim-bookworm
ARG RUNTIME=debian:bookworm-slim

FROM ${BASE} AS build
WORKDIR /src
COPY Cargo.toml ./
COPY src ./src
# Scorers are Intel Xeon Cascade Lake or newer: x86-64-v3 (AVX2) is always available there.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release

FROM ${RUNTIME}
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/sessions /app/sessions
WORKDIR /app
# Let glibc's malloc back large allocations with transparent huge pages (fewer page faults).
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1
# Runs with no arguments; reads DATA_DIR (/data), writes OUTPUT_DIR (/output).
CMD ["/app/sessions"]
