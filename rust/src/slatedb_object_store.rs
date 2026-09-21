use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use object_store_opendal::OpendalStore;
use opendal::Operator;

#[derive(Debug)]
pub(crate) struct SlateDbObjectStore {
    inner: OpendalStore,
}

impl SlateDbObjectStore {
    pub(crate) fn new(operator: Operator) -> Self {
        Self {
            inner: OpendalStore::new(operator),
        }
    }
}

impl std::fmt::Display for SlateDbObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(formatter)
    }
}

fn map_conditional_get_result(
    result: object_store::Result<GetResult>,
    not_modified: bool,
) -> object_store::Result<GetResult> {
    match result {
        Err(ObjectStoreError::Precondition { path, source }) if not_modified => {
            Err(ObjectStoreError::NotModified { path, source })
        }
        result => result,
    }
}

#[async_trait]
impl ObjectStore for SlateDbObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let not_modified = options.if_match.is_none()
            && options.if_unmodified_since.is_none()
            && (options.if_none_match.is_some() || options.if_modified_since.is_some());
        map_conditional_get_result(self.inner.get_opts(location, options).await, not_modified)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conditional_get_errors_follow_object_store_semantics() {
        fn precondition_error() -> ObjectStoreError {
            ObjectStoreError::Precondition {
                path: "conditional-get".to_string(),
                source: Box::new(std::io::Error::other("condition failed")),
            }
        }

        let error = map_conditional_get_result(Err(precondition_error()), true)
            .expect_err("negative condition should report not modified");
        assert!(
            matches!(error, ObjectStoreError::NotModified { .. }),
            "unexpected error: {error:?}"
        );

        let error = map_conditional_get_result(Err(precondition_error()), false)
            .expect_err("positive condition should remain a precondition error");
        assert!(matches!(error, ObjectStoreError::Precondition { .. }));
    }
}
