#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "slatedb_file_handle.hpp"
#include "slatedb_file_system.hpp"
#include "duckdb/common/exception.hpp"

#include <array>

using namespace duckdb;

namespace {

unique_ptr<FileHandle> CreateFile(SlateDBFileSystem &fs, const string &path) {
	return fs.OpenFile(path,
	                   FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
}

} // namespace

TEST_CASE("SlateDBFileHandle owns and closes its FFI handle", "[slatedb_fs]") {
	SlateDBFileSystem fs;
	auto handle = CreateFile(fs, "slatedb://close.db");

	REQUIRE(handle);
	REQUIRE_NOTHROW(handle->Cast<SlateDBFileHandle>());
	REQUIRE_NOTHROW(handle->Close());
	REQUIRE_NOTHROW(handle->Close());
}

TEST_CASE("SlateDBFileSystem maps DuckDB open flags", "[slatedb_fs]") {
	SlateDBFileSystem fs;
	const string path = "slatedb://flags.db";

	REQUIRE(fs.CanHandleFile("slatedb://flags.db"));
	REQUIRE(fs.CanHandleFile("slatedb:/flags.db"));
	REQUIRE(fs.CanonicalizePath(path) == path);
	auto missing = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
	REQUIRE(!missing);
	REQUIRE_THROWS_AS(fs.OpenFile(path, FileFlags::FILE_FLAGS_READ), IOException);

	auto handle = CreateFile(fs, path);
	std::array<uint8_t, 3> initial {'a', 'b', 'c'};
	REQUIRE(fs.Write(*handle, initial.data(), initial.size()) == 3);
	handle->Close();

	handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	REQUIRE(fs.GetFileSize(*handle) == 0);
	handle->Close();

	handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
	                               FileFlags::FILE_FLAGS_APPEND);
	std::array<uint8_t, 1> suffix {'x'};
	REQUIRE(fs.Write(*handle, suffix.data(), suffix.size()) == 1);
	handle->Close();
	handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	REQUIRE(fs.GetFileSize(*handle) == 1);
}

TEST_CASE("SlateDBFileSystem forwards sequential IO and seeking", "[slatedb_fs]") {
	SlateDBFileSystem fs;
	auto handle = CreateFile(fs, "slatedb://sequential.db");
	std::array<uint8_t, 4> input {'a', 'b', 'c', 'd'};

	REQUIRE(fs.Write(*handle, input.data(), input.size()) == 4);
	REQUIRE(fs.GetFileSize(*handle) == 4);
	REQUIRE(fs.CanSeek());
	REQUIRE(!fs.OnDiskFile(*handle));

	fs.Seek(*handle, 1);
	REQUIRE(fs.SeekPosition(*handle) == 1);
	std::array<uint8_t, 2> output {};
	std::array<uint8_t, 2> expected {'b', 'c'};
	REQUIRE(fs.Read(*handle, output.data(), output.size()) == 2);
	REQUIRE(output == expected);
	REQUIRE(fs.SeekPosition(*handle) == 3);
	handle->Reset();
	REQUIRE(fs.SeekPosition(*handle) == 0);
}

TEST_CASE("SlateDBFileSystem forwards positional IO", "[slatedb_fs]") {
	SlateDBFileSystem fs;
	auto handle = CreateFile(fs, "slatedb://positional.db");
	std::array<uint8_t, 4> initial {'a', 'b', 'c', 'd'};
	fs.Write(*handle, initial.data(), initial.size(), 0);

	std::array<uint8_t, 2> replacement {'X', 'Y'};
	fs.Write(*handle, replacement.data(), replacement.size(), 1);
	std::array<uint8_t, 4> output {};
	std::array<uint8_t, 4> expected {'a', 'X', 'Y', 'd'};
	fs.Read(*handle, output.data(), output.size(), 0);

	REQUIRE(output == expected);
	REQUIRE_THROWS_AS(fs.Read(*handle, output.data(), output.size(), 2), IOException);
}

TEST_CASE("SlateDBFileSystem forwards sync and truncate", "[slatedb_fs]") {
	SlateDBFileSystem fs;
	const string path = "slatedb://truncate.db";
	auto handle = CreateFile(fs, path);
	std::array<uint8_t, 4> input {'a', 'b', 'c', 'd'};

	REQUIRE(fs.Write(*handle, input.data(), input.size()) == 4);
	REQUIRE_NOTHROW(fs.FileSync(*handle));
	REQUIRE_NOTHROW(fs.Truncate(*handle, 2));
	REQUIRE(fs.GetFileSize(*handle) == 2);
	REQUIRE_NOTHROW(fs.FileSync(*handle));
	handle->Close();

	handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	std::array<uint8_t, 2> output {};
	std::array<uint8_t, 2> expected {'a', 'b'};
	fs.Read(*handle, output.data(), output.size(), 0);
	REQUIRE(output == expected);
}

TEST_CASE("SlateDBFileSystem forwards file catalog operations", "[slatedb_fs]") {
	SlateDBFileSystem fs;
	const string source = "slatedb://source.db";
	const string target = "slatedb://target.db";

	REQUIRE(!fs.FileExists(source));
	CreateFile(fs, source)->Close();
	REQUIRE(fs.FileExists(source));

	REQUIRE_NOTHROW(fs.MoveFile(source, target));
	REQUIRE(!fs.FileExists(source));
	REQUIRE(fs.FileExists(target));

	REQUIRE_NOTHROW(fs.RemoveFile(target));
	REQUIRE(!fs.FileExists(target));
	REQUIRE_THROWS_AS(fs.RemoveFile(target), IOException);
}

TEST_CASE("SlateDBFileSystem converts FFI errors to DuckDB exceptions", "[slatedb_fs]") {
	SlateDBFileSystem fs;
	auto writable = CreateFile(fs, "slatedb://readonly.db");
	writable->Close();
	auto read_only = fs.OpenFile("slatedb://readonly.db", FileFlags::FILE_FLAGS_READ);
	std::array<uint8_t, 1> byte {'x'};

	REQUIRE_THROWS_AS(fs.Write(*read_only, byte.data(), byte.size()), IOException);
	REQUIRE_THROWS_AS(fs.Truncate(*read_only, 0), IOException);
}
