use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Instant;

use opendal::raw::oio;
use opendal::raw::*;
use opendal::{Buffer, BytesRange, Capability, Metadata, OperationContext, Result};

use crate::io_metrics::IoMetrics;

#[derive(Clone)]
pub(crate) struct IoMetricsLayer {
    metrics: Arc<IoMetrics>,
}

impl IoMetricsLayer {
    pub(crate) fn new(metrics: Arc<IoMetrics>) -> Self {
        Self { metrics }
    }
}

impl Debug for IoMetricsLayer {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("IoMetricsLayer").finish()
    }
}

impl Layer for IoMetricsLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(IoMetricsService {
            inner,
            metrics: Arc::clone(&self.metrics),
        })
    }
}

struct IoMetricsService {
    inner: Servicer,
    metrics: Arc<IoMetrics>,
}

impl Debug for IoMetricsService {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("IoMetricsService").finish()
    }
}

impl Service for IoMetricsService {
    type Reader = IoMetricsReader;
    type Writer = IoMetricsWriter;
    type Lister = IoMetricsLister;
    type Deleter = IoMetricsDeleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        context: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(context, path, args).await
    }

    async fn stat(&self, context: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let start = Instant::now();
        let result = self.inner.stat(context, path, args).await;
        self.metrics.record_stat(start.elapsed());
        result
    }

    fn read(&self, context: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let start = Instant::now();
        match self.inner.read(context, path, args) {
            Ok(inner) => Ok(IoMetricsReader {
                inner,
                metrics: Arc::clone(&self.metrics),
            }),
            Err(error) => {
                self.metrics.record_read(start.elapsed());
                Err(error)
            }
        }
    }

    fn write(&self, context: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let start = Instant::now();
        match self.inner.write(context, path, args) {
            Ok(inner) => Ok(IoMetricsWriter {
                inner,
                metrics: Arc::clone(&self.metrics),
                start,
                recorded: false,
            }),
            Err(error) => {
                self.metrics.record_write(start.elapsed());
                Err(error)
            }
        }
    }

    fn delete(&self, context: &OperationContext) -> Result<Self::Deleter> {
        let start = Instant::now();
        match self.inner.delete(context) {
            Ok(inner) => Ok(IoMetricsDeleter {
                inner,
                metrics: Arc::clone(&self.metrics),
                start,
                recorded: false,
            }),
            Err(error) => {
                self.metrics.record_delete(start.elapsed());
                Err(error)
            }
        }
    }

    fn list(&self, context: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let start = Instant::now();
        match self.inner.list(context, path, args) {
            Ok(inner) => Ok(IoMetricsLister {
                inner,
                metrics: Arc::clone(&self.metrics),
                start,
                recorded: false,
            }),
            Err(error) => {
                self.metrics.record_list(start.elapsed());
                Err(error)
            }
        }
    }

    fn copy(
        &self,
        context: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        self.inner.copy(context, from, to, args)
    }

    fn compose(
        &self,
        context: &OperationContext,
        to: &str,
        args: OpCompose,
    ) -> Result<Self::Composer> {
        self.inner.compose(context, to, args)
    }

    async fn rename(
        &self,
        context: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(context, from, to, args).await
    }

    async fn restore(
        &self,
        context: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        self.inner.restore(context, path, args).await
    }

    async fn presign(
        &self,
        context: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(context, path, args).await
    }
}

struct IoMetricsReader {
    inner: oio::Reader,
    metrics: Arc<IoMetrics>,
}

impl oio::Read for IoMetricsReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let start = Instant::now();
        match self.inner.open(range).await {
            Ok((response, stream)) => Ok((
                response,
                Box::new(IoMetricsReadStream {
                    inner: stream,
                    metrics: Arc::clone(&self.metrics),
                    start,
                    recorded: false,
                }),
            )),
            Err(error) => {
                self.metrics.record_read(start.elapsed());
                Err(error)
            }
        }
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let start = Instant::now();
        let result = self.inner.read(range).await;
        self.metrics.record_read(start.elapsed());
        result
    }
}

struct IoMetricsReadStream {
    inner: Box<dyn oio::ReadStreamDyn>,
    metrics: Arc<IoMetrics>,
    start: Instant,
    recorded: bool,
}

impl IoMetricsReadStream {
    fn record_once(&mut self) {
        if !self.recorded {
            self.metrics.record_read(self.start.elapsed());
            self.recorded = true;
        }
    }
}

impl Drop for IoMetricsReadStream {
    fn drop(&mut self) {
        self.record_once();
    }
}

impl oio::ReadStream for IoMetricsReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        let result = self.inner.read().await;
        if !matches!(&result, Ok(buffer) if !buffer.is_empty()) {
            self.record_once();
        }
        result
    }
}

struct IoMetricsWriter {
    inner: oio::Writer,
    metrics: Arc<IoMetrics>,
    start: Instant,
    recorded: bool,
}

impl IoMetricsWriter {
    fn record_once(&mut self) {
        if !self.recorded {
            self.metrics.record_write(self.start.elapsed());
            self.recorded = true;
        }
    }
}

impl Drop for IoMetricsWriter {
    fn drop(&mut self) {
        self.record_once();
    }
}

impl oio::Write for IoMetricsWriter {
    async fn write(&mut self, buffer: Buffer) -> Result<()> {
        self.inner.write(buffer).await
    }

    async fn copy_from(&mut self, path: &str, args: OpRead, range: BytesRange) -> Result<()> {
        self.inner.copy_from(path, args, range).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        let result = self.inner.close().await;
        self.record_once();
        result
    }

    async fn abort(&mut self) -> Result<()> {
        let result = self.inner.abort().await;
        self.record_once();
        result
    }
}

struct IoMetricsDeleter {
    inner: oio::Deleter,
    metrics: Arc<IoMetrics>,
    start: Instant,
    recorded: bool,
}

impl IoMetricsDeleter {
    fn record_once(&mut self) {
        if !self.recorded {
            self.metrics.record_delete(self.start.elapsed());
            self.recorded = true;
        }
    }
}

impl Drop for IoMetricsDeleter {
    fn drop(&mut self) {
        self.record_once();
    }
}

impl oio::Delete for IoMetricsDeleter {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.inner.delete(path, args).await
    }

    async fn close(&mut self) -> Result<()> {
        let result = self.inner.close().await;
        self.record_once();
        result
    }
}

struct IoMetricsLister {
    inner: oio::Lister,
    metrics: Arc<IoMetrics>,
    start: Instant,
    recorded: bool,
}

impl IoMetricsLister {
    fn record_once(&mut self) {
        if !self.recorded {
            self.metrics.record_list(self.start.elapsed());
            self.recorded = true;
        }
    }
}

impl Drop for IoMetricsLister {
    fn drop(&mut self) {
        self.record_once();
    }
}

impl oio::List for IoMetricsLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let result = self.inner.next().await;
        if !matches!(&result, Ok(Some(_))) {
            self.record_once();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::services::Memory;
    use opendal::Operator;

    #[tokio::test]
    async fn records_supported_operation_metrics() {
        let metrics = Arc::new(IoMetrics::default());
        let operator = Operator::new(Memory::default())
            .expect("memory operator")
            .layer(IoMetricsLayer::new(Arc::clone(&metrics)));

        operator
            .write("metrics-test", "hello")
            .await
            .expect("write");
        operator.read("metrics-test").await.expect("read");
        operator.stat("metrics-test").await.expect("stat");
        operator.list("/").await.expect("list");
        operator.delete("metrics-test").await.expect("delete");

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.write.request_count, 1);
        assert_eq!(snapshot.read.request_count, 1);
        assert_eq!(snapshot.stat.request_count, 1);
        assert_eq!(snapshot.list.request_count, 1);
        assert_eq!(snapshot.delete.request_count, 1);
    }
}
