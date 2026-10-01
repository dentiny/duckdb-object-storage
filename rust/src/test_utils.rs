use std::path::Path;

use crate::{FileOpenFlags, SlateDbFileSystem};

pub(crate) async fn open_local_fs(
    database_path: &str,
    root: impl AsRef<Path>,
) -> SlateDbFileSystem {
    SlateDbFileSystem::open_local(database_path, root)
        .await
        .expect("open local filesystem")
}

pub(crate) async fn write_file(fs: &SlateDbFileSystem, path: &str, contents: &[u8]) {
    let mut file = fs
        .open_file(path, FileOpenFlags::open_or_create())
        .await
        .expect("open file for writing");
    file.pwrite(contents, 0).await.expect("write file");
    file.close().await.expect("close file");
}

pub(crate) async fn read_file(fs: &SlateDbFileSystem, path: &str, len: usize) -> Vec<u8> {
    let mut file = fs
        .open_file(path, FileOpenFlags::read_only())
        .await
        .expect("open file for reading");
    let mut contents = vec![0; len];
    assert_eq!(file.pread(&mut contents, 0).await.expect("read file"), len);
    file.close().await.expect("close file");
    contents
}
