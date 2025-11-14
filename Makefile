.PHONY: clean clean_all

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=dns

# Set to 1 to enable Unstable API (binaries will only work on TARGET_DUCKDB_VERSION, forwards compatibility will be broken)
# Note: currently extension-template-rs requires this, as duckdb-rs relies on unstable C API functionality
USE_UNSTABLE_C_API=1

# Target DuckDB version
TARGET_DUCKDB_VERSION=v1.4.2

all: configure debug

# Include makefiles from DuckDB
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

# Override test targets for Windows to exclude performance.test
ifeq ($(DUCKDB_PLATFORM),windows_amd64)
test_extension_release_internal:
	@echo "Running RELEASE tests (excluding performance.test on Windows).."
	@$(TEST_RUNNER) --test-dir test/sql --file-path dns.test --external-extension build/release/$(EXTENSION_NAME).duckdb_extension
	@$(TEST_RUNNER) --test-dir test/sql --file-path cache_performance.test --external-extension build/release/$(EXTENSION_NAME).duckdb_extension

test_extension_debug_internal:
	@echo "Running DEBUG tests (excluding performance.test on Windows).."
	@$(TEST_RUNNER) --test-dir test/sql --file-path dns.test --external-extension build/debug/$(EXTENSION_NAME).duckdb_extension
	@$(TEST_RUNNER) --test-dir test/sql --file-path cache_performance.test --external-extension build/debug/$(EXTENSION_NAME).duckdb_extension
endif

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_rust
clean_all: clean_configure clean
