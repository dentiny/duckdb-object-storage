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

.PHONY: format-all
