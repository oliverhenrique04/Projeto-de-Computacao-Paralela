"""
Camada de persistência — SQLite com WAL mode para suporte a múltiplos processos.
Cada processo cria sua própria conexão (seguro com multiprocessing + fork).
O UNIQUE INDEX em reservations é a garantia final contra dupla reserva.
"""
import os
import sqlite3
from contextlib import contextmanager

# Variável de ambiente para o dev de distribuição apontar para outro host/path
DB_PATH = os.environ.get('CONNECTONGS_DB', 'connectongs.db')


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")      # múltiplos leitores simultâneos
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")    # espera até 10s por lock do BD
    return conn


@contextmanager
def cursor():
    conn = get_conn()
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    conn = get_conn()
    # executescript faz commit automático — ideal para DDL
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            name          TEXT    NOT NULL,
            email         TEXT    UNIQUE NOT NULL,
            password_hash TEXT    NOT NULL,
            user_type     TEXT    NOT NULL
                CHECK(user_type IN ('RESTAURANTE','ONG','ADMIN')),
            region        TEXT    NOT NULL,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS foods (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            restaurant_id INTEGER NOT NULL REFERENCES users(id),
            name          TEXT    NOT NULL,
            category      TEXT    NOT NULL,
            quantity      INTEGER NOT NULL,
            expiry_date   DATE    NOT NULL,
            region        TEXT    NOT NULL,
            status        TEXT    NOT NULL DEFAULT 'DISPONIVEL'
                CHECK(status IN ('DISPONIVEL','RESERVADO','EXPIRADO','CANCELADO')),
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS reservations (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            food_id    INTEGER NOT NULL REFERENCES foods(id),
            ong_id     INTEGER NOT NULL REFERENCES users(id),
            status     TEXT NOT NULL DEFAULT 'PENDENTE'
                CHECK(status IN ('PENDENTE','CONCLUIDO','CANCELADO')),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Garante que não existam duas reservas ativas para o mesmo alimento.
        -- Esta é a última linha de defesa contra race conditions — o Lock da
        -- aplicação age antes, mas este índice protege mesmo sem o Lock.
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_active_reservation
            ON reservations(food_id) WHERE status != 'CANCELADO';

        CREATE TABLE IF NOT EXISTS notifications (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL REFERENCES users(id),
            message    TEXT NOT NULL,
            read       INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Trilha de auditoria para rastreabilidade (LGPD)
        CREATE TABLE IF NOT EXISTS audit_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            user_id    INTEGER,
            details    TEXT,
            process_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.close()


def audit(event_type: str, user_id: int = None, details: str = None):
    with cursor() as cur:
        cur.execute(
            "INSERT INTO audit_log(event_type, user_id, details, process_id) VALUES(?,?,?,?)",
            (event_type, user_id, details, os.getpid())
        )
