all: format build

build:
	@cargo build 

format:
	@cargo fmt -- --write-mode overwrite

clean:
	@cargo clean