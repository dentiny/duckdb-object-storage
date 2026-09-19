PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=duckdb_object_storage
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

CMAKE_FILES := CMakeLists.txt extension_config.cmake

format-all: format
	cmake-format -i $(CMAKE_FILES)
	cargo fmt --manifest-path rust/Cargo.toml

test-s3:
	bash test/s3/run_e2e.sh

# Allow a DuckDB test filter as a positional make argument, for example:
# make test_reldebug_duckdb "[wal]" "[disk]"
DUCKDB_TEST_TARGETS := test_debug_duckdb test_reldebug_duckdb test_release_duckdb
DUCKDB_TEST_CONFIG_ARGS := [memory] [disk]
DUCKDB_TEST_ARGUMENTS := $(filter-out $(DUCKDB_TEST_TARGETS),$(MAKECMDGOALS))
DUCKDB_TEST_FILTER ?= $(filter-out $(DUCKDB_TEST_CONFIG_ARGS),$(DUCKDB_TEST_ARGUMENTS))
DUCKDB_TEST_DEFAULT_CONFIG := test/configs/duckdb_slatedb.json
DUCKDB_TEST_CONFIG ?= $(if $(filter [memory],$(DUCKDB_TEST_ARGUMENTS)),test/configs/duckdb_slatedb_memory.json,$(if $(filter [disk],$(DUCKDB_TEST_ARGUMENTS)),test/configs/duckdb_slatedb_disk.json,$(DUCKDB_TEST_DEFAULT_CONFIG)))
ifneq ($(filter $(DUCKDB_TEST_TARGETS),$(MAKECMDGOALS)),)
ifneq ($(word 2,$(filter $(DUCKDB_TEST_CONFIG_ARGS),$(DUCKDB_TEST_ARGUMENTS))),)
$(error Specify only one DuckDB test config: "[memory]" or "[disk]")
endif
ifneq ($(strip $(DUCKDB_TEST_ARGUMENTS)),)
.PHONY: $(DUCKDB_TEST_ARGUMENTS)
$(DUCKDB_TEST_ARGUMENTS):
	@:
endif
endif

test_debug_duckdb:
	./build/debug/test/unittest --test-config $(DUCKDB_TEST_CONFIG) --test-dir duckdb $(if $(DUCKDB_TEST_FILTER),"$(DUCKDB_TEST_FILTER)")

test_reldebug_duckdb:
	./build/reldebug/test/unittest --test-config $(DUCKDB_TEST_CONFIG) --test-dir duckdb $(if $(DUCKDB_TEST_FILTER),"$(DUCKDB_TEST_FILTER)")

test_release_duckdb:
	./build/release/test/unittest --test-config $(DUCKDB_TEST_CONFIG) --test-dir duckdb $(if $(DUCKDB_TEST_FILTER),"$(DUCKDB_TEST_FILTER)")

.PHONY: format-all test-s3 $(DUCKDB_TEST_TARGETS)
