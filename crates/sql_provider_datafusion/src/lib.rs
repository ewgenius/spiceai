#![allow(clippy::missing_errors_doc)]

use async_trait::async_trait;
use db_connection_pool::dbconnection::{get_schema, query_arrow};
use db_connection_pool::DbConnectionPool;
use futures::TryStreamExt;
use snafu::prelude::*;
use std::{any::Any, fmt, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::OwnedTableReference,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{context::SessionState, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::{
        project_schema, stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType,
        ExecutionPlan, SendableRecordBatchStream,
    },
};

pub mod expr;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to get a DB connection from the pool: {source}"))]
    UnableToGetConnectionFromPool { source: db_connection_pool::Error },

    #[snafu(display("Unable to get schema: {source}"))]
    UnableToGetSchema {
        source: db_connection_pool::dbconnection::Error,
    },

    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQL { source: expr::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SqlTable<T: 'static, P: 'static> {
    pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
    schema: SchemaRef,
    table_reference: OwnedTableReference,
}

impl<T, P> SqlTable<T, P> {
    pub async fn new(
        pool: &Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        table_reference: impl Into<OwnedTableReference>,
    ) -> Result<Self> {
        let table_reference = table_reference.into();
        let conn = pool
            .connect()
            .await
            .context(UnableToGetConnectionFromPoolSnafu)?;

        let schema = get_schema(conn, &table_reference)
            .await
            .context(UnableToGetSchemaSnafu)?;

        Ok(Self {
            pool: Arc::clone(pool),
            schema,
            table_reference,
        })
    }

    pub fn new_with_schema(
        pool: &Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<OwnedTableReference>,
    ) -> Self {
        Self {
            pool: Arc::clone(pool),
            schema: schema.into(),
            table_reference: table_reference.into(),
        }
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SqlExec::new(
            projections,
            schema,
            &self.table_reference,
            Arc::clone(&self.pool),
            filters,
            limit,
        )?))
    }
}

#[async_trait]
impl<T, P> TableProvider for SqlTable<T, P> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let mut filter_push_down = vec![];
        for filter in filters {
            match expr::to_sql(filter) {
                Ok(_) => filter_push_down.push(TableProviderFilterPushDown::Exact),
                Err(_) => filter_push_down.push(TableProviderFilterPushDown::Unsupported),
            }
        }

        Ok(filter_push_down)
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, &self.schema(), filters, limit);
    }
}

#[derive(Clone)]
struct SqlExec<T, P> {
    projected_schema: SchemaRef,
    table_reference: OwnedTableReference,
    pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl<T, P> SqlExec<T, P> {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &OwnedTableReference,
        pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema(schema, projections)?;
        Ok(Self {
            projected_schema,
            table_reference: table_reference.clone(),
            pool,
            filters: filters.to_vec(),
            limit,
        })
    }

    fn sql(&self) -> Result<String> {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect::<Vec<_>>()
            .join(", ");

        let limit_expr = match self.limit {
            Some(limit) => format!("LIMIT {limit}"),
            None => String::new(),
        };

        let where_expr = if self.filters.is_empty() {
            String::new()
        } else {
            let filter_expr = self
                .filters
                .iter()
                .map(expr::to_sql)
                .collect::<expr::Result<Vec<_>>>()
                .context(UnableToGenerateSQLSnafu)?;
            format!("WHERE {}", filter_expr.join(" AND "))
        };

        Ok(format!(
            "SELECT {columns} FROM {table_reference} {where_expr} {limit_expr}",
            table_reference = self.table_reference,
        ))
    }
}

impl<T, P> std::fmt::Debug for SqlExec<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "SqlExec sql={sql}")
    }
}

impl<T, P> DisplayAs for SqlExec<T, P> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "SqlExec sql={sql}")
    }
}

impl<T: 'static, P: 'static> ExecutionPlan for SqlExec<T, P> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let sql = self.sql().map_err(to_execution_error)?;
        tracing::debug!("SqlExec sql: {sql}");

        let fut = get_stream(Arc::clone(&self.pool), sql);

        let stream = futures::stream::once(fut).try_flatten();
        let schema = self.schema().clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

async fn get_stream<T: 'static, P: 'static>(
    pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
    sql: String,
) -> DataFusionResult<SendableRecordBatchStream> {
    let conn = pool.connect().await.map_err(to_execution_error)?;

    query_arrow(conn, sql).await.map_err(to_execution_error)
}

#[allow(clippy::needless_pass_by_value)]
fn to_execution_error(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use datafusion::execution::context::SessionContext;
    use db_connection_pool::dbconnection::duckdbconn::DuckDbConnection;
    use db_connection_pool::{duckdbpool::DuckDbConnectionPool, DbConnectionPool, Mode};
    use duckdb::{DuckdbConnectionManager, ToSql};
    use tracing::{level_filters::LevelFilter, subscriber::DefaultGuard, Dispatch};

    use crate::SqlTable;

    fn setup_tracing() -> DefaultGuard {
        let subscriber: tracing_subscriber::FmtSubscriber = tracing_subscriber::fmt()
            .with_max_level(LevelFilter::DEBUG)
            .finish();

        let dispatch = Dispatch::new(subscriber);
        tracing::dispatcher::set_default(&dispatch)
    }

    #[tokio::test]
    async fn test_duckdb_table() -> Result<(), Box<dyn Error + Send + Sync>> {
        let t = setup_tracing();
        let ctx = SessionContext::new();
        let pool: Arc<
            dyn DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, &dyn ToSql>
                + Send
                + Sync,
        > = Arc::new(DuckDbConnectionPool::new(
            "test",
            &Mode::Memory,
            &Arc::new(Option::None),
        )?);
        let conn = pool.connect().await?;
        let db_conn = conn
            .as_any()
            .downcast_ref::<DuckDbConnection>()
            .expect("Unable to downcast to DuckDbConnection");
        db_conn.conn.execute_batch(
            "CREATE TABLE test (a INTEGER, b VARCHAR); INSERT INTO test VALUES (3, 'bar');",
        )?;
        let duckdb_table = SqlTable::new(&pool, "test").await?;
        ctx.register_table("test_datafusion", Arc::new(duckdb_table))?;
        let sql = "SELECT * FROM test_datafusion limit 1";
        let df = ctx.sql(sql).await?;
        df.show().await?;
        drop(t);
        Ok(())
    }

    #[tokio::test]
    async fn test_duckdb_table_filter() -> Result<(), Box<dyn Error + Send + Sync>> {
        let t = setup_tracing();
        let ctx = SessionContext::new();
        let pool: Arc<
            dyn DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, &dyn ToSql>
                + Send
                + Sync,
        > = Arc::new(DuckDbConnectionPool::new(
            "test",
            &Mode::Memory,
            &Arc::new(Option::None),
        )?);
        let conn = pool.connect().await?;
        let db_conn = conn
            .as_any()
            .downcast_ref::<DuckDbConnection>()
            .expect("Unable to downcast to DuckDbConnection");
        db_conn.conn.execute_batch(
            "CREATE TABLE test (a INTEGER, b VARCHAR); INSERT INTO test VALUES (3, 'bar');",
        )?;
        let duckdb_table = SqlTable::new(&pool, "test").await?;
        ctx.register_table("test_datafusion", Arc::new(duckdb_table))?;
        let sql = "SELECT * FROM test_datafusion where a > 1 and b = 'bar' limit 1";
        let df = ctx.sql(sql).await?;
        df.show().await?;
        drop(t);
        Ok(())
    }
}
