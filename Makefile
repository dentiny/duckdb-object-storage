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
#
# DuckDB tests that cannot pass against the extension are listed in
# test/configs/skip/{extension_bugs,unsupported_features,by_design}.json, which every
# SlateDB test config extends. Maintain them with:
#   make test_release_duckdb_skipped "[memory]"  # fails if a skip-listed test passes now
#   make test_release_duckdb_survey "[memory]"   # one process per test, reports uncovered failures
#   make lint_duckdb_skip_lists
# Set DUCKDB_SKIP_LISTS (e.g. "extension_bugs") to restrict *_skipped to some lists.
DUCKDB_BUILD_TYPES := debug reldebug release
DUCKDB_TEST_TARGETS := $(foreach build,$(DUCKDB_BUILD_TYPES),test_$(build)_duckdb test_$(build)_duckdb_skipped test_$(build)_duckdb_survey)
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

define RUN_DUCKDB_TESTS
TEST_PID=; \
cleanup() { \
	if [ -n "$$TEST_PID" ]; then \
		rm -rf "$(PROJ_DIR)duckdb/duckdb_unittest_tempdir/$$TEST_PID" \
			"$(PROJ_DIR)duckdb/duckdb_unittest_tempdir/$${TEST_PID}_slatedb"; \
	fi; \
}; \
terminate() { \
	if [ -n "$$TEST_PID" ]; then kill "$$TEST_PID" 2>/dev/null || true; fi; \
}; \
trap cleanup EXIT; \
trap terminate INT TERM; \
./build/$(1)/test/unittest --test-config $(DUCKDB_TEST_CONFIG) --test-dir duckdb \
	$(if $(DUCKDB_TEST_FILTER),"$(DUCKDB_TEST_FILTER)") & \
TEST_PID=$$!; \
wait "$$TEST_PID"
endef

test_debug_duckdb:
	@$(call RUN_DUCKDB_TESTS,debug)

test_reldebug_duckdb:
	@$(call RUN_DUCKDB_TESTS,reldebug)

test_release_duckdb:
	@$(call RUN_DUCKDB_TESTS,release)

DUCKDB_TEST_SKIPS := python3 scripts/duckdb_test_skips.py
DUCKDB_SKIP_LISTS ?=
# The skip lists are shared, so by default check that each entry still fails under some config.
DUCKDB_SKIPPED_CONFIGS ?= $(if $(filter $(DUCKDB_TEST_CONFIG_ARGS),$(DUCKDB_TEST_ARGUMENTS)),$(DUCKDB_TEST_CONFIG),test/configs/duckdb_slatedb_disk.json test/configs/duckdb_slatedb_memory.json)

$(foreach build,$(DUCKDB_BUILD_TYPES),test_$(build)_duckdb_skipped):
	$(DUCKDB_TEST_SKIPS) skipped --unittest ./build/$(patsubst test_%_duckdb_skipped,%,$@)/test/unittest \
		$(foreach config,$(DUCKDB_SKIPPED_CONFIGS),--config $(config)) $(DUCKDB_SKIP_LISTS)

$(foreach build,$(DUCKDB_BUILD_TYPES),test_$(build)_duckdb_survey):
	$(DUCKDB_TEST_SKIPS) survey --unittest ./build/$(patsubst test_%_duckdb_survey,%,$@)/test/unittest \
		--config $(DUCKDB_TEST_CONFIG) --output build/duckdb_test_survey.json \
		$(if $(DUCKDB_TEST_FILTER),--filter "$(DUCKDB_TEST_FILTER)")

lint_duckdb_skip_lists:
	$(DUCKDB_TEST_SKIPS) lint $(if $(wildcard ./build/release/test/unittest),--unittest ./build/release/test/unittest)

.PHONY: format-all test-s3 lint_duckdb_skip_lists $(DUCKDB_TEST_TARGETS)
