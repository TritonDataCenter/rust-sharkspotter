#
# Copyright 2020 Joyent, Inc
#

#
# Variables
#

NAME = rust-sharkspotter
CARGO ?= cargo
RUST_CLIPPY_ARGS ?= -- -D clippy::all

#
# Repo-specific targets
#
.PHONY: all
all: build-sharkspotter

.PHONY: build-sharkspotter
build-sharkspotter:
	$(CARGO) build --release

.PHONY: test
test:
	$(CARGO) test

.PHONY: check
check:
	$(CARGO) clean && $(CARGO) clippy $(RUST_CLIPPY_ARGS)
