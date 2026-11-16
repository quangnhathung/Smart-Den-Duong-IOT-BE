"""All connections/transactions are owned by the service worker."""

import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
 node_id TEXT PRIMARY KEY, boot_id TEXT, last_seen REAL, payload TEXT
);
CREATE TABLE IF NOT EXISTS boots (
 node_id TEXT NOT NULL, boot_id TEXT NOT NULL, PRIMARY KEY(node_id,boot_id)
);
CREATE TABLE IF NOT EXISTS telemetry (
 id INTEGER PRIMARY KEY, node_id TEXT NOT NULL REFERENCES nodes(node_id), boot_id TEXT NOT NULL,
 seq TEXT NOT NULL, uptime_ms TEXT NOT NULL, received_at REAL NOT NULL,
 payload TEXT NOT NULL, quality TEXT NOT NULL, continuity TEXT NOT NULL,
 UNIQUE(node_id,boot_id,seq)
);
CREATE INDEX IF NOT EXISTS telemetry_time ON telemetry(node_id,received_at,id);
CREATE TABLE IF NOT EXISTS availability (
 id INTEGER PRIMARY KEY, node_id TEXT NOT NULL, boot_id TEXT NOT NULL,
 status TEXT NOT NULL, received_at REAL NOT NULL, session TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS alerts (
 id INTEGER PRIMARY KEY, node_id TEXT NOT NULL REFERENCES nodes(node_id), kind TEXT NOT NULL,
 started_at REAL NOT NULL, ended_at REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS active_alert ON alerts(node_id,kind) WHERE ended_at IS NULL;
CREATE TABLE IF NOT EXISTS commands (
 command_id TEXT PRIMARY KEY, node_id TEXT NOT NULL REFERENCES nodes(node_id), boot_id TEXT NOT NULL,
 payload TEXT NOT NULL, created_at REAL NOT NULL, sent_at REAL, retry_at REAL,
 result TEXT NOT NULL, reason TEXT, ack_at REAL, attempts INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS pending_node ON commands(node_id) WHERE result='PENDING';
CREATE TABLE IF NOT EXISTS ack_events (
 id INTEGER PRIMARY KEY, command_id TEXT NOT NULL, received_at REAL NOT NULL,
 payload TEXT NOT NULL, disposition TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS experiments (
 id INTEGER PRIMARY KEY, name TEXT NOT NULL, configuration TEXT NOT NULL, notes TEXT NOT NULL,
 started_at REAL NOT NULL, ended_at REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS one_experiment ON experiments((1)) WHERE ended_at IS NULL;
CREATE TABLE IF NOT EXISTS ingest_errors (
 id INTEGER PRIMARY KEY, received_at REAL NOT NULL, topic TEXT NOT NULL, reason TEXT NOT NULL, payload BLOB NOT NULL
);
"""


def open_database(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=1)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA busy_timeout=1000")
    db.execute("PRAGMA journal_mode=WAL")
    version = db.execute("PRAGMA user_version").fetchone()[0]
    if version not in (0, 1):
        db.close()
        raise RuntimeError("unsupported database schema version")
    db.executescript(SCHEMA)
    with db:
        db.executemany("INSERT OR IGNORE INTO nodes(node_id) VALUES (?)", [(f"node_0{i}",) for i in (1, 2, 3)])
        db.execute("PRAGMA user_version=1")
    return db


class DatabaseLock:
    """OS advisory lock prevents a second collector using the same database; crash releases it."""

    def __init__(self, path):
        import os

        self.file = None
        path.parent.mkdir(parents=True, exist_ok=True)
        stream = open(str(path) + ".lock", "a+b")
        try:
            if os.name == "nt":
                import msvcrt

                if stream.seek(0, 2) == 0:
                    stream.write(b"0")
                    stream.flush()
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            stream.close()
            raise RuntimeError("Database already owned by another backend; run one worker") from error
        self.file = stream

    def close(self):
        import os

        if self.file is None:
            return
        if os.name == "nt":
            import msvcrt

            self.file.seek(0)
            msvcrt.locking(self.file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(self.file.fileno(), fcntl.LOCK_UN)
        self.file.close()
        self.file = None
