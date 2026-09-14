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

test-cpp:
	$(MAKE) release EXT_FLAGS="$(EXT_FLAGS) -DDUCKDB_OBJECT_STORAGE_BUILD_CPP_TESTS=ON"
	cmake --build build/release --target check_duckdb_object_storage_cpp

test: test-cpp

.PHONY: format-all test-cpp
