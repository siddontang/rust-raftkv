all: format build

build:
	@cargo +nightly build --release
	@mkdir -p ./bin
	@cp target/release/raftkv ./bin/raftkv

format:
	@cargo fmt 

clean:
	@cargo clean