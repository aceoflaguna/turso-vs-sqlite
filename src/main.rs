//! dbbench — stress test / benchmark of SQLite (via rusqlite) vs Turso Database.
//!
//! Runs the same deterministic workloads against each engine, then writes
//! `results.json` and a self-contained `report.html`.
//!
//! Engines:
//!   sqlite      SQLite (bundled) through rusqlite, WAL journal
//!   turso       Turso Database, default journal mode
//!   turso-mvcc  Turso Database, MVCC journal mode + BEGIN CONCURRENT writers

use clap::Parser;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Keep in sync with Cargo.toml (shown in the report header).
const TURSO_VERSION: &str = "0.8.3-pre.1";

const INSERT_SQL: &str = "INSERT INTO users (id, name, email, age, balance, country, bio) \
                          VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";
const SELECT_BY_ID_SQL: &str =
    "SELECT id, name, email, age, balance, country, bio FROM users WHERE id = ?1";
const RANGE_SQL: &str = "SELECT id, name, age FROM users WHERE age BETWEEN ?1 AND ?2 LIMIT 100";
const UPDATE_SQL: &str = "UPDATE users SET balance = balance + ?1, age = ?2 WHERE id = ?3";
const BUMP_SQL: &str = "UPDATE users SET balance = balance + 1.0 WHERE id = ?1";
const AGG_SQL: &str = "SELECT country, COUNT(*), AVG(balance), MAX(age) FROM users \
                       GROUP BY country ORDER BY COUNT(*) DESC";
const TOPN_SQL: &str = "SELECT id, name, balance FROM users ORDER BY balance DESC LIMIT 10";

const UPDATE_BATCH: usize = 100;

// ───────────────────────────── CLI ─────────────────────────────

#[derive(Parser, Clone, Debug)]
#[command(version, about = "Stress test SQLite vs Turso Database and generate an HTML stats report")]
struct Args {
    /// Engines to run, comma separated: sqlite, turso, turso-mvcc
    #[arg(long, default_value = "sqlite,turso,turso-mvcc")]
    engines: String,
    /// Rows in the main table
    #[arg(long, default_value_t = 100_000)]
    rows: usize,
    /// Rows per transaction in the bulk insert
    #[arg(long, default_value_t = 1_000)]
    batch: usize,
    /// Single-row autocommit INSERTs (one fsync-bound commit each)
    #[arg(long, default_value_t = 2_000)]
    autocommit_inserts: usize,
    /// Random primary-key point reads
    #[arg(long, default_value_t = 50_000)]
    reads: usize,
    /// Indexed range queries
    #[arg(long, default_value_t = 5_000)]
    range_scans: usize,
    /// Random-row UPDATEs (batched 100 per transaction)
    #[arg(long, default_value_t = 20_000)]
    updates: usize,
    /// Runs of each full-scan query (GROUP BY, ORDER BY..LIMIT)
    #[arg(long, default_value_t = 20)]
    agg_runs: usize,
    /// Worker counts for the concurrent mixed test, comma separated
    #[arg(long, default_value = "1,2,4,8")]
    concurrency: String,
    /// Seconds per concurrency level
    #[arg(long, default_value_t = 5)]
    duration: u64,
    /// Percentage of reads in the concurrent mixed workload
    #[arg(long, default_value_t = 80)]
    read_pct: u32,
    /// Rows seeded for each concurrent run (capped at --rows)
    #[arg(long, default_value_t = 50_000)]
    conc_rows: usize,
    /// PRAGMA synchronous value: off | normal | full
    #[arg(long, default_value = "normal")]
    synchronous: String,
    /// HTML report path
    #[arg(long, default_value = "report.html")]
    out: PathBuf,
    /// Raw results JSON path
    #[arg(long, default_value = "results.json")]
    json: PathBuf,
    /// Directory for the scratch database files (default: system temp dir)
    #[arg(long)]
    dir: Option<PathBuf>,
    /// RNG seed (identical data and access pattern for every engine)
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Small, fast run for smoke testing
    #[arg(long)]
    quick: bool,
}

// ───────────────────────────── engines ─────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum EngineKind {
    Sqlite,
    Turso,
    TursoMvcc,
}

impl EngineKind {
    fn id(self) -> &'static str {
        match self {
            EngineKind::Sqlite => "sqlite",
            EngineKind::Turso => "turso",
            EngineKind::TursoMvcc => "turso-mvcc",
        }
    }
    fn label(self) -> &'static str {
        match self {
            EngineKind::Sqlite => "SQLite",
            EngineKind::Turso => "Turso",
            EngineKind::TursoMvcc => "Turso (MVCC)",
        }
    }
    fn description(self) -> String {
        match self {
            EngineKind::Sqlite => format!(
                "SQLite {} via rusqlite (bundled), WAL journal, one OS thread per worker",
                rusqlite::version()
            ),
            EngineKind::Turso => format!(
                "Turso Database {TURSO_VERSION}, default journal, async tasks on tokio"
            ),
            EngineKind::TursoMvcc => format!(
                "Turso Database {TURSO_VERSION}, journal_mode=mvcc, BEGIN CONCURRENT writers"
            ),
        }
    }
    fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "sqlite" => Some(EngineKind::Sqlite),
            "turso" => Some(EngineKind::Turso),
            "turso-mvcc" | "mvcc" => Some(EngineKind::TursoMvcc),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct DbErr {
    msg: String,
    retryable: bool,
}

fn err(msg: impl Into<String>) -> DbErr {
    DbErr { msg: msg.into(), retryable: false }
}

impl From<rusqlite::Error> for DbErr {
    fn from(e: rusqlite::Error) -> Self {
        let retryable = matches!(
            e.sqlite_error_code(),
            Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
        );
        DbErr { msg: e.to_string(), retryable }
    }
}

impl From<turso::Error> for DbErr {
    fn from(e: turso::Error) -> Self {
        let msg = e.to_string();
        let lower = msg.to_lowercase();
        let retryable = matches!(e, turso::Error::Busy(_) | turso::Error::BusySnapshot(_))
            || lower.contains("conflict")
            || lower.contains("busy");
        DbErr { msg, retryable }
    }
}

/// A bind parameter. Borrowed text keeps the SQLite path allocation-free;
/// Turso's API takes owned values, so it copies (each engine uses its idiomatic API).
#[derive(Clone, Copy)]
enum Val<'a> {
    I(i64),
    F(f64),
    T(&'a str),
}

fn sq_val<'a>(v: &Val<'a>) -> rusqlite::types::ToSqlOutput<'a> {
    use rusqlite::types::{ToSqlOutput, ValueRef};
    ToSqlOutput::Borrowed(match *v {
        Val::I(i) => ValueRef::Integer(i),
        Val::F(f) => ValueRef::Real(f),
        Val::T(s) => ValueRef::Text(s.as_bytes()),
    })
}

fn tu_val(v: &Val<'_>) -> turso::Value {
    match *v {
        Val::I(i) => turso::Value::Integer(i),
        Val::F(f) => turso::Value::Real(f),
        Val::T(s) => turso::Value::Text(s.to_string()),
    }
}

enum Conn {
    Sqlite(rusqlite::Connection),
    Turso { conn: turso::Connection, mvcc: bool },
}

impl Conn {
    async fn exec(&mut self, sql: &str, p: &[Val<'_>]) -> Result<u64, DbErr> {
        match self {
            Conn::Sqlite(c) => {
                let mut st = c.prepare_cached(sql)?;
                let n = st.execute(rusqlite::params_from_iter(p.iter().map(sq_val)))?;
                Ok(n as u64)
            }
            Conn::Turso { conn, .. } => {
                let mut st = conn.prepare_cached(sql).await?;
                let n = st.execute(turso::params_from_iter(p.iter().map(tu_val))).await?;
                Ok(n)
            }
        }
    }

    /// Runs a query, touches every column of every row, returns the row count.
    async fn query(&mut self, sql: &str, p: &[Val<'_>]) -> Result<u64, DbErr> {
        match self {
            Conn::Sqlite(c) => {
                let mut st = c.prepare_cached(sql)?;
                let ncols = st.column_count();
                let mut rows = st.query(rusqlite::params_from_iter(p.iter().map(sq_val)))?;
                let mut n = 0u64;
                while let Some(row) = rows.next()? {
                    for i in 0..ncols {
                        std::hint::black_box(row.get_ref(i)?);
                    }
                    n += 1;
                }
                Ok(n)
            }
            Conn::Turso { conn, .. } => {
                let mut st = conn.prepare_cached(sql).await?;
                let mut rows = st.query(turso::params_from_iter(p.iter().map(tu_val))).await?;
                let mut n = 0u64;
                while let Some(row) = rows.next().await? {
                    for i in 0..row.column_count() {
                        std::hint::black_box(row.get_value(i)?);
                    }
                    n += 1;
                }
                Ok(n)
            }
        }
    }

    async fn run(&mut self, sql: &str) -> Result<(), DbErr> {
        match self {
            Conn::Sqlite(c) => {
                c.execute_batch(sql)?;
                Ok(())
            }
            Conn::Turso { conn, .. } => {
                conn.execute(sql, ()).await?;
                Ok(())
            }
        }
    }

    async fn begin_write(&mut self) -> Result<(), DbErr> {
        let sql = match self {
            Conn::Sqlite(_) => "BEGIN IMMEDIATE",
            Conn::Turso { mvcc: true, .. } => "BEGIN CONCURRENT",
            Conn::Turso { .. } => "BEGIN",
        };
        self.run(sql).await
    }

    async fn commit(&mut self) -> Result<(), DbErr> {
        self.run("COMMIT").await
    }

    async fn rollback(&mut self) {
        let _ = self.run("ROLLBACK").await;
    }
}

#[derive(Clone)]
enum Db {
    Sqlite { path: PathBuf },
    Turso { db: turso::Database, mvcc: bool },
}

async fn open_db(
    kind: EngineKind,
    path: &Path,
    args: &Args,
    notes: &mut Vec<String>,
) -> Result<Db, DbErr> {
    match kind {
        EngineKind::Sqlite => {
            let c = rusqlite::Connection::open(path)?;
            let mode: String = c.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
            if !mode.eq_ignore_ascii_case("wal") {
                notes.push(format!("sqlite: journal_mode is '{mode}', expected 'wal'"));
            }
            Ok(Db::Sqlite { path: path.to_path_buf() })
        }
        EngineKind::Turso | EngineKind::TursoMvcc => {
            let mvcc = kind == EngineKind::TursoMvcc;
            let p = path.to_str().ok_or_else(|| err("non-UTF-8 db path"))?;
            let db = turso::Builder::new_local(p).build().await?;
            let c = db.connect()?;
            if mvcc {
                c.pragma_update("journal_mode", "'mvcc'").await?;
            }
            if let Err(e) = c.pragma_update("synchronous", &args.synchronous).await {
                notes.push(format!(
                    "{}: PRAGMA synchronous={} was not applied ({e}); engine default used",
                    kind.id(),
                    args.synchronous
                ));
            }
            Ok(Db::Turso { db, mvcc })
        }
    }
}

impl Db {
    async fn connect(&self, args: &Args) -> Result<Conn, DbErr> {
        match self {
            Db::Sqlite { path } => {
                let c = rusqlite::Connection::open(path)?;
                c.busy_timeout(Duration::from_secs(10))?;
                c.pragma_update(None, "synchronous", &args.synchronous)?;
                Ok(Conn::Sqlite(c))
            }
            Db::Turso { db, mvcc } => {
                let c = db.connect()?;
                let _ = c.busy_timeout(Duration::from_secs(10));
                let _ = c.pragma_update("synchronous", &args.synchronous).await;
                Ok(Conn::Turso { conn: c, mvcc: *mvcc })
            }
        }
    }
}

async fn create_schema(conn: &mut Conn) -> Result<(), DbErr> {
    conn.run(
        "CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER NOT NULL,
            balance REAL NOT NULL,
            country TEXT NOT NULL,
            bio TEXT NOT NULL
        )",
    )
    .await?;
    conn.run("CREATE INDEX IF NOT EXISTS idx_users_age ON users(age)").await?;
    Ok(())
}

// ───────────────────────────── data ─────────────────────────────

struct UserRow {
    name: String,
    email: String,
    age: i64,
    balance: f64,
    country: String,
    bio: String,
}

const COUNTRIES: [&str; 20] = [
    "US", "CN", "IN", "BR", "NG", "ID", "PK", "BD", "RU", "MX", "JP", "PH", "EG", "ET", "VN", "DE",
    "TR", "IR", "FR", "GB",
];

fn gen_rows(n: usize, seed: u64) -> Vec<UserRow> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|i| {
            let bio_len = rng.random_range(60..160);
            let bio: String =
                (0..bio_len).map(|_| (b'a' + rng.random_range(0..26u8)) as char).collect();
            UserRow {
                name: format!("user_{i}"),
                email: format!("user_{i}@example.com"),
                age: rng.random_range(18..=80),
                balance: rng.random_range(0.0..100_000.0),
                country: COUNTRIES[rng.random_range(0..COUNTRIES.len())].to_string(),
                bio,
            }
        })
        .collect()
}

fn insert_params<'a>(id: i64, r: &'a UserRow) -> [Val<'a>; 7] {
    [
        Val::I(id),
        Val::T(&r.name),
        Val::T(&r.email),
        Val::I(r.age),
        Val::F(r.balance),
        Val::T(&r.country),
        Val::T(&r.bio),
    ]
}

// ───────────────────────────── metrics ─────────────────────────────

#[derive(Serialize, Clone, Default)]
struct Metric {
    ops: u64,
    secs: f64,
    ops_per_sec: f64,
    mean_us: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    max_us: f64,
    errors: u64,
    retries: u64,
}

fn summarize(mut lat: Vec<Duration>, ops: u64, secs: f64, errors: u64, retries: u64) -> Metric {
    lat.sort_unstable();
    let pct = |p: f64| -> f64 {
        if lat.is_empty() {
            return 0.0;
        }
        let idx = ((p / 100.0) * lat.len() as f64).ceil() as usize;
        lat[idx.clamp(1, lat.len()) - 1].as_secs_f64() * 1e6
    };
    let mean_us = if lat.is_empty() {
        0.0
    } else {
        lat.iter().map(|d| d.as_secs_f64()).sum::<f64>() / lat.len() as f64 * 1e6
    };
    Metric {
        ops,
        secs,
        ops_per_sec: if secs > 0.0 { ops as f64 / secs } else { 0.0 },
        mean_us,
        p50_us: pct(50.0),
        p95_us: pct(95.0),
        p99_us: pct(99.0),
        max_us: lat.last().map(|d| d.as_secs_f64() * 1e6).unwrap_or(0.0),
        errors,
        retries,
    }
}

#[derive(Serialize)]
struct WorkloadResult {
    engine: String,
    workload: String,
    title: String,
    description: String,
    latency_basis: String,
    unit: String,
    status: String,
    error: Option<String>,
    metric: Option<Metric>,
}

#[derive(Serialize)]
struct ConcResult {
    engine: String,
    workers: usize,
    status: String,
    error: Option<String>,
    secs: f64,
    total_ops: u64,
    ops_per_sec: f64,
    reads: Option<Metric>,
    writes: Option<Metric>,
    retries: u64,
    errors: u64,
}

#[derive(Serialize)]
struct EngineMeta {
    id: String,
    label: String,
    description: String,
}

#[derive(Serialize)]
struct Meta {
    generated_unix: u64,
    cpus: usize,
    os: String,
    arch: String,
    sqlite_version: String,
    turso_version: String,
    db_dir: String,
    config: serde_json::Value,
    notes: Vec<String>,
    engines: Vec<EngineMeta>,
}

#[derive(Serialize)]
struct Report {
    meta: Meta,
    workloads: Vec<WorkloadResult>,
    concurrency: Vec<ConcResult>,
}

// ───────────────────────────── sequential workloads ─────────────────────────────

#[derive(Clone, Copy)]
enum Wl {
    BulkInsert,
    InsertAutocommit,
    PointRead,
    RangeScan,
    UpdateBatched,
    Aggregate,
    TopN,
}

struct WlInfo {
    name: &'static str,
    title: &'static str,
    description: String,
    basis: String,
    unit: &'static str,
}

impl Wl {
    const ALL: [Wl; 7] = [
        Wl::BulkInsert,
        Wl::InsertAutocommit,
        Wl::PointRead,
        Wl::RangeScan,
        Wl::UpdateBatched,
        Wl::Aggregate,
        Wl::TopN,
    ];

    fn info(self, a: &Args) -> WlInfo {
        match self {
            Wl::BulkInsert => WlInfo {
                name: "bulk_insert",
                title: "Bulk insert",
                description: format!(
                    "{} rows (7 columns + secondary index) in {}-row transactions",
                    a.rows, a.batch
                ),
                basis: format!("per {}-row transaction", a.batch),
                unit: "rows/s",
            },
            Wl::InsertAutocommit => WlInfo {
                name: "insert_autocommit",
                title: "Autocommit insert",
                description: format!(
                    "{} single-row INSERTs, each its own transaction",
                    a.autocommit_inserts
                ),
                basis: "per INSERT (includes commit)".into(),
                unit: "inserts/s",
            },
            Wl::PointRead => WlInfo {
                name: "point_read",
                title: "Point read",
                description: format!("{} random primary-key lookups, all columns", a.reads),
                basis: "per query".into(),
                unit: "queries/s",
            },
            Wl::RangeScan => WlInfo {
                name: "range_scan",
                title: "Indexed range scan",
                description: format!(
                    "{} range queries on indexed age (3-year window, LIMIT 100)",
                    a.range_scans
                ),
                basis: "per query".into(),
                unit: "queries/s",
            },
            Wl::UpdateBatched => WlInfo {
                name: "update_batched",
                title: "Batched update",
                description: format!(
                    "{} random-row UPDATEs touching an indexed column, {} per transaction",
                    a.updates, UPDATE_BATCH
                ),
                basis: format!("per {UPDATE_BATCH}-update transaction"),
                unit: "updates/s",
            },
            Wl::Aggregate => WlInfo {
                name: "aggregate_group_by",
                title: "Aggregate (GROUP BY)",
                description: format!(
                    "{} full-table GROUP BY country with COUNT, AVG, MAX",
                    a.agg_runs
                ),
                basis: "per query".into(),
                unit: "queries/s",
            },
            Wl::TopN => WlInfo {
                name: "order_by_top_n",
                title: "Top-N sort",
                description: format!(
                    "{} full-scan ORDER BY balance DESC LIMIT 10 queries",
                    a.agg_runs
                ),
                basis: "per query".into(),
                unit: "queries/s",
            },
        }
    }
}

#[derive(Clone)]
struct Ctx {
    args: Arc<Args>,
    data: Arc<Vec<UserRow>>,
}

async fn bulk_insert(
    conn: &mut Conn,
    data: &[UserRow],
    batch: usize,
    lat: &mut Vec<Duration>,
) -> Result<(), DbErr> {
    for (ci, chunk) in data.chunks(batch.max(1)).enumerate() {
        let t = Instant::now();
        conn.begin_write().await?;
        for (j, r) in chunk.iter().enumerate() {
            let id = (ci * batch.max(1) + j + 1) as i64;
            if let Err(e) = conn.exec(INSERT_SQL, &insert_params(id, r)).await {
                conn.rollback().await;
                return Err(e);
            }
        }
        conn.commit().await?;
        lat.push(t.elapsed());
    }
    Ok(())
}

async fn run_wl(wl: Wl, conn: &mut Conn, ctx: &Ctx) -> Result<Metric, DbErr> {
    let a = &*ctx.args;
    let data = &ctx.data;
    let n = data.len();
    let mut rng = StdRng::seed_from_u64(a.seed.wrapping_add(wl as u64 * 1000 + 7));

    match wl {
        Wl::BulkInsert => {
            create_schema(conn).await?;
            let mut lat = Vec::new();
            let t = Instant::now();
            bulk_insert(conn, data, a.batch, &mut lat).await?;
            Ok(summarize(lat, n as u64, t.elapsed().as_secs_f64(), 0, 0))
        }
        Wl::InsertAutocommit => {
            let m = a.autocommit_inserts;
            let mut lat = Vec::with_capacity(m);
            let t = Instant::now();
            for i in 0..m {
                let r = &data[i % n];
                let id = (n + i + 1) as i64;
                let s = Instant::now();
                conn.exec(INSERT_SQL, &insert_params(id, r)).await?;
                lat.push(s.elapsed());
            }
            Ok(summarize(lat, m as u64, t.elapsed().as_secs_f64(), 0, 0))
        }
        Wl::PointRead => {
            let ids: Vec<i64> = (0..a.reads).map(|_| rng.random_range(1..=n as i64)).collect();
            let mut lat = Vec::with_capacity(ids.len());
            let mut misses = 0u64;
            let t = Instant::now();
            for id in ids {
                let s = Instant::now();
                if conn.query(SELECT_BY_ID_SQL, &[Val::I(id)]).await? != 1 {
                    misses += 1;
                }
                lat.push(s.elapsed());
            }
            let ops = lat.len() as u64;
            Ok(summarize(lat, ops, t.elapsed().as_secs_f64(), misses, 0))
        }
        Wl::RangeScan => {
            let ranges: Vec<i64> = (0..a.range_scans).map(|_| rng.random_range(18..=77)).collect();
            let mut lat = Vec::with_capacity(ranges.len());
            let t = Instant::now();
            for lo in ranges {
                let s = Instant::now();
                conn.query(RANGE_SQL, &[Val::I(lo), Val::I(lo + 2)]).await?;
                lat.push(s.elapsed());
            }
            let ops = lat.len() as u64;
            Ok(summarize(lat, ops, t.elapsed().as_secs_f64(), 0, 0))
        }
        Wl::UpdateBatched => {
            let targets: Vec<(i64, i64)> = (0..a.updates)
                .map(|_| (rng.random_range(1..=n as i64), rng.random_range(18..=80)))
                .collect();
            let mut lat = Vec::new();
            let t = Instant::now();
            for chunk in targets.chunks(UPDATE_BATCH) {
                let s = Instant::now();
                conn.begin_write().await?;
                for &(id, age) in chunk {
                    if let Err(e) =
                        conn.exec(UPDATE_SQL, &[Val::F(1.5), Val::I(age), Val::I(id)]).await
                    {
                        conn.rollback().await;
                        return Err(e);
                    }
                }
                conn.commit().await?;
                lat.push(s.elapsed());
            }
            Ok(summarize(lat, targets.len() as u64, t.elapsed().as_secs_f64(), 0, 0))
        }
        Wl::Aggregate | Wl::TopN => {
            let sql = if matches!(wl, Wl::Aggregate) { AGG_SQL } else { TOPN_SQL };
            let mut lat = Vec::with_capacity(a.agg_runs);
            let t = Instant::now();
            for _ in 0..a.agg_runs {
                let s = Instant::now();
                conn.query(sql, &[]).await?;
                lat.push(s.elapsed());
            }
            let ops = lat.len() as u64;
            Ok(summarize(lat, ops, t.elapsed().as_secs_f64(), 0, 0))
        }
    }
}

fn panic_msg(e: tokio::task::JoinError) -> String {
    if e.is_panic() {
        let p = e.into_panic();
        let m = p
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| p.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".into());
        format!("panic: {m}")
    } else {
        format!("task failed: {e}")
    }
}

async fn run_sequential(
    kind: EngineKind,
    args: &Arc<Args>,
    data: &Arc<Vec<UserRow>>,
    dir: &Path,
    notes: &mut Vec<String>,
) -> Vec<WorkloadResult> {
    let path = dir.join(format!("{}_seq.db", kind.id()));
    let db = open_db(kind, &path, args, notes).await;
    let mut out = Vec::new();

    for wl in Wl::ALL {
        let info = wl.info(args);
        eprintln!("  [{}] {} ...", kind.id(), info.name);
        let res: Result<Metric, String> = match &db {
            Err(e) => Err(format!("could not open database: {}", e.msg)),
            Ok(db) => {
                let db = db.clone();
                let ctx = Ctx { args: args.clone(), data: data.clone() };
                let h = tokio::spawn(async move {
                    let mut conn = db.connect(&ctx.args).await?;
                    run_wl(wl, &mut conn, &ctx).await
                });
                match h.await {
                    Ok(Ok(m)) => Ok(m),
                    Ok(Err(e)) => Err(e.msg),
                    Err(je) => Err(panic_msg(je)),
                }
            }
        };
        match &res {
            Ok(m) => eprintln!(
                "      {:>12.0} {}  p50 {:.0}us  p99 {:.0}us",
                m.ops_per_sec, info.unit, m.p50_us, m.p99_us
            ),
            Err(e) => eprintln!("      FAILED: {e}"),
        }
        out.push(WorkloadResult {
            engine: kind.id().into(),
            workload: info.name.into(),
            title: info.title.into(),
            description: info.description,
            latency_basis: info.basis,
            unit: info.unit.into(),
            status: if res.is_ok() { "ok" } else { "failed" }.into(),
            error: res.as_ref().err().cloned(),
            metric: res.ok(),
        });
    }
    out
}

// ───────────────────────────── concurrent mixed workload ─────────────────────────────

#[derive(Default)]
struct WorkerOut {
    reads: Vec<Duration>,
    writes: Vec<Duration>,
    errors: u64,
    retries: u64,
    first_error: Option<String>,
}

impl WorkerOut {
    fn note(&mut self, msg: String) {
        self.errors += 1;
        if self.first_error.is_none() {
            self.first_error = Some(msg);
        }
    }
}

async fn write_txn(conn: &mut Conn, id: i64) -> Result<(), DbErr> {
    conn.begin_write().await?;
    conn.exec(BUMP_SQL, &[Val::I(id)]).await?;
    conn.commit().await
}

async fn worker(
    db: Db,
    args: Arc<Args>,
    seed: u64,
    n_rows: i64,
    deadline: Instant,
) -> WorkerOut {
    let mut out = WorkerOut::default();
    let mut conn = match db.connect(&args).await {
        Ok(c) => c,
        Err(e) => {
            out.note(e.msg);
            return out;
        }
    };
    let mut rng = StdRng::seed_from_u64(seed);

    while Instant::now() < deadline {
        let is_read = rng.random_range(0..100u32) < args.read_pct;
        let id = rng.random_range(1..=n_rows);
        let s = Instant::now();
        if is_read {
            match conn.query(SELECT_BY_ID_SQL, &[Val::I(id)]).await {
                Ok(_) => out.reads.push(s.elapsed()),
                Err(e) => out.note(e.msg),
            }
        } else {
            let mut attempts = 0;
            loop {
                match write_txn(&mut conn, id).await {
                    Ok(()) => {
                        out.writes.push(s.elapsed());
                        break;
                    }
                    Err(e) if e.retryable && attempts < 1_000 => {
                        attempts += 1;
                        out.retries += 1;
                        conn.rollback().await;
                        tokio::task::yield_now().await;
                    }
                    Err(e) => {
                        out.note(e.msg);
                        conn.rollback().await;
                        break;
                    }
                }
            }
        }
        if out.errors > 100 {
            break;
        }
    }
    out
}

async fn run_concurrency(
    kind: EngineKind,
    workers: usize,
    args: &Arc<Args>,
    data: &Arc<Vec<UserRow>>,
    dir: &Path,
    notes: &mut Vec<String>,
) -> ConcResult {
    let failed = |msg: String| ConcResult {
        engine: kind.id().into(),
        workers,
        status: "failed".into(),
        error: Some(msg),
        secs: 0.0,
        total_ops: 0,
        ops_per_sec: 0.0,
        reads: None,
        writes: None,
        retries: 0,
        errors: 0,
    };

    let n_rows = args.conc_rows.min(data.len());
    let path = dir.join(format!("{}_c{}.db", kind.id(), workers));

    // Setup (untimed): fresh database seeded with n_rows.
    let setup: Result<Db, DbErr> = async {
        let db = open_db(kind, &path, args, notes).await?;
        let mut c = db.connect(args).await?;
        create_schema(&mut c).await?;
        let mut lat = Vec::new();
        bulk_insert(&mut c, &data[..n_rows], 1_000, &mut lat).await?;
        Ok(db)
    }
    .await;
    let db = match setup {
        Ok(db) => db,
        Err(e) => return failed(format!("setup failed: {}", e.msg)),
    };

    let handle = tokio::runtime::Handle::current();
    let start = Instant::now();
    let deadline = start + Duration::from_secs(args.duration);
    let mut joins = Vec::new();
    for w in 0..workers {
        let db = db.clone();
        let args = args.clone();
        let seed = args.seed.wrapping_mul(31).wrapping_add(w as u64 + 1);
        let n = n_rows as i64;
        if kind == EngineKind::Sqlite {
            // Blocking SQLite calls get real OS threads so workers truly run in parallel.
            let h = handle.clone();
            joins.push(tokio::task::spawn_blocking(move || {
                h.block_on(worker(db, args, seed, n, deadline))
            }));
        } else {
            joins.push(tokio::spawn(worker(db, args, seed, n, deadline)));
        }
    }

    let mut reads = Vec::new();
    let mut writes = Vec::new();
    let (mut errors, mut retries) = (0u64, 0u64);
    let mut first_error: Option<String> = None;
    for j in joins {
        match j.await {
            Ok(o) => {
                reads.extend(o.reads);
                writes.extend(o.writes);
                errors += o.errors;
                retries += o.retries;
                if first_error.is_none() {
                    first_error = o.first_error;
                }
            }
            Err(je) => {
                errors += 1;
                first_error.get_or_insert(panic_msg(je));
            }
        }
    }
    let secs = start.elapsed().as_secs_f64();
    let (nr, nw) = (reads.len() as u64, writes.len() as u64);
    let total = nr + nw;

    if total == 0 {
        return failed(first_error.unwrap_or_else(|| "no operations completed".into()));
    }

    ConcResult {
        engine: kind.id().into(),
        workers,
        status: "ok".into(),
        error: first_error,
        secs,
        total_ops: total,
        ops_per_sec: total as f64 / secs,
        reads: Some(summarize(reads, nr, secs, 0, 0)),
        writes: Some(summarize(writes, nw, secs, 0, retries)),
        retries,
        errors,
    }
}

// ───────────────────────────── main ─────────────────────────────

fn parse_list<T>(s: &str, f: impl Fn(&str) -> Option<T>, what: &str) -> Vec<T> {
    s.split(',')
        .filter(|x| !x.trim().is_empty())
        .map(|x| {
            f(x).unwrap_or_else(|| {
                eprintln!("invalid {what}: '{x}'");
                std::process::exit(2);
            })
        })
        .collect()
}

#[tokio::main]
async fn main() {
    let mut args = Args::parse();
    if args.quick {
        args.rows = 10_000;
        args.autocommit_inserts = 300;
        args.reads = 5_000;
        args.range_scans = 1_000;
        args.updates = 2_000;
        args.agg_runs = 5;
        args.concurrency = "1,4".into();
        args.duration = 2;
        args.conc_rows = 5_000;
    }
    if !matches!(args.synchronous.as_str(), "off" | "normal" | "full") {
        eprintln!("--synchronous must be one of: off, normal, full");
        std::process::exit(2);
    }
    let engines = parse_list(&args.engines, EngineKind::parse, "engine");
    let levels = parse_list(&args.concurrency, |s| s.trim().parse::<usize>().ok().filter(|n| *n > 0), "worker count");
    if engines.is_empty() || args.rows == 0 {
        eprintln!("need at least one engine and --rows > 0");
        std::process::exit(2);
    }

    let base = args.dir.clone().unwrap_or_else(std::env::temp_dir);
    let tmp = tempfile::Builder::new()
        .prefix("dbbench-")
        .tempdir_in(&base)
        .expect("create scratch directory");
    let dir = tmp.path().to_path_buf();

    eprintln!(
        "dbbench: {} rows, engines [{}], synchronous={}, scratch dir {}",
        args.rows,
        engines.iter().map(|e| e.id()).collect::<Vec<_>>().join(", "),
        args.synchronous,
        dir.display()
    );

    let args = Arc::new(args);
    let data = Arc::new(gen_rows(args.rows, args.seed));
    let mut notes: Vec<String> = Vec::new();
    let mut workloads = Vec::new();
    let mut concurrency = Vec::new();

    eprintln!("\n== Sequential workloads ==");
    for &kind in &engines {
        eprintln!("{}", kind.label());
        workloads.extend(run_sequential(kind, &args, &data, &dir, &mut notes).await);
    }

    eprintln!("\n== Concurrent mixed workload ({}% reads, {}s per level) ==", args.read_pct, args.duration);
    for &workers in &levels {
        for &kind in &engines {
            eprint!("  [{}] {} workers ... ", kind.id(), workers);
            let r = run_concurrency(kind, workers, &args, &data, &dir, &mut notes).await;
            match r.status.as_str() {
                "ok" => eprintln!(
                    "{:.0} ops/s ({} retries, {} errors)",
                    r.ops_per_sec, r.retries, r.errors
                ),
                _ => eprintln!("FAILED: {}", r.error.as_deref().unwrap_or("?")),
            }
            concurrency.push(r);
        }
    }

    notes.sort();
    notes.dedup();

    let meta = Meta {
        generated_unix: SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0),
        cpus: std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1),
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        sqlite_version: rusqlite::version().into(),
        turso_version: TURSO_VERSION.into(),
        db_dir: dir.display().to_string(),
        config: serde_json::json!({
            "rows": args.rows,
            "batch": args.batch,
            "autocommit_inserts": args.autocommit_inserts,
            "reads": args.reads,
            "range_scans": args.range_scans,
            "updates": args.updates,
            "agg_runs": args.agg_runs,
            "concurrency": levels,
            "duration_secs": args.duration,
            "read_pct": args.read_pct,
            "conc_rows": args.conc_rows.min(args.rows),
            "synchronous": args.synchronous,
            "seed": args.seed,
        }),
        notes,
        engines: engines
            .iter()
            .map(|e| EngineMeta {
                id: e.id().into(),
                label: e.label().into(),
                description: e.description(),
            })
            .collect(),
    };
    let report = Report { meta, workloads, concurrency };

    let json = serde_json::to_string_pretty(&report).expect("serialize results");
    std::fs::write(&args.json, &json).expect("write results.json");

    let template = include_str!("report_template.html");
    let embedded = serde_json::to_string(&report).expect("serialize").replace("</", "<\\/");
    let html = template.replace("/*__DATA__*/null", &embedded);
    std::fs::write(&args.out, html).expect("write report.html");

    eprintln!("\nwrote {} and {}", args.out.display(), args.json.display());
}
