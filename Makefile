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

.PHONY: libtest
libtest:
	$(CARGO) test --lib

.PHONY: integrationtest 
integrationtest:
	$(CARGO) test --test integration

# for jenkins to run lib tests and a subset of integration tests
.PHONY: prepush
prepush: libtest
	$(CARGO) test --test integration cli
	
.PHONY: test
test: libtest integrationtest

.PHONY: check
check:
	$(CARGO) clean && $(CARGO) clippy $(RUST_CLIPPY_ARGS)
