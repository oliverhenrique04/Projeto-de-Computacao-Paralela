"""
CONNECTONGS — Semeador de Banco de Dados
Computação Paralela e Distribuída — Fase 2

Cria 1000 usuários de teste no banco SQLite usando multiprocessing.Pool
para paralelizar o hashing bcrypt (operação CPU-bound).

Dados gerados:
  •   50 restaurantes → restaurante{01..50}@connectongs.dev
  •  950 ONGs         → ong{0001..0950}@connectongs.dev
  •  200 alimentos    → 4 por restaurante, validades variadas
  Senha única para todos: Teste@1234

Por que rounds=4?
  rounds=12 (padrão de produção) = ~303ms/hash × 1000 = 5 minutos
  rounds=4  (suficiente para testes) =   ~1ms/hash × 1000 = ~1 segundo
  O hash bcrypt armazena os rounds internamente — checkpw() funciona igual.

Uso:
    python3 seed_database.py              # cria banco padrão
    python3 seed_database.py --status     # mostra contagem atual no banco
    python3 seed_database.py --reset      # apaga e recria tudo
"""

import multiprocessing
import os
import sys
import time
from datetime import date, timedelta
import sqlite3

try:
    import bcrypt
except ImportError:
    print("[ERRO] bcrypt não instalado. Execute: pip install bcrypt==4.2.1")
    sys.exit(1)

# ─── Configuração ─────────────────────────────────────────────────────────────

N_RESTAURANTS = 50
N_ONGS = 950
N_FOODS_PER_RESTAURANT = 4
SEED_PASSWORD = 'Teste@1234'
BCRYPT_ROUNDS = 4    # suficiente para testes; login funciona normalmente

DB_PATH = os.environ.get('CONNECTONGS_DB', 'connectongs.db')

FOOD_NAMES = [
    "Marmitas Executivas", "Saladas Mistas", "Sopa Minestrone",
    "Frango Grelhado", "Arroz e Feijão", "Pizza Margherita",
    "Pão de Forma", "Bolo de Chocolate", "Frutas Sortidas",
    "Legumes Cozidos", "Macarrão ao Molho", "Torta Salgada",
]
FOOD_CATEGORIES = ["Refeição", "Lanche", "Sopa", "Proteína", "Vegano", "Sobremesa"]
REGIONS = [
    "Plano Piloto", "Taguatinga", "Ceilândia", "Sobradinho",
    "Gama", "Samambaia", "Santa Maria", "Recanto das Emas",
]


# ─── Workers de hashing paralelo ──────────────────────────────────────────────

def _hash_user(args) -> tuple:
    """
    Top-level (picklable) para Pool.map.
    Recebe (name, email, password, user_type, region) → devolve tupla com hash.
    """
    name, email, password, user_type, region = args
    pw_hash = bcrypt.hashpw(
        password.encode(),
        bcrypt.gensalt(rounds=BCRYPT_ROUNDS)
    ).decode()
    return (name, email, pw_hash, user_type, region)


# ─── Banco de dados ───────────────────────────────────────────────────────────

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _init_schema(conn: sqlite3.Connection):
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

        CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_active_reservation
            ON reservations(food_id) WHERE status != 'CANCELADO';

        CREATE TABLE IF NOT EXISTS notifications (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL REFERENCES users(id),
            message    TEXT NOT NULL,
            read       INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS audit_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            user_id    INTEGER,
            details    TEXT,
            process_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()


def _current_counts(conn: sqlite3.Connection) -> dict:
    cur = conn.cursor()
    cur.execute("SELECT user_type, COUNT(*) AS c FROM users GROUP BY user_type")
    users = {r['user_type']: r['c'] for r in cur.fetchall()}
    cur.execute("SELECT COUNT(*) AS c FROM foods")
    foods = cur.fetchone()['c']
    return {**users, 'FOODS': foods}


# ─── Seed principal ───────────────────────────────────────────────────────────

def build_user_specs() -> list:
    """Monta a lista de (name, email, password, user_type, region) para todos os 1000 usuários."""
    specs = []

    for i in range(1, N_RESTAURANTS + 1):
        region = REGIONS[(i - 1) % len(REGIONS)]
        specs.append((
            f"Restaurante {i:02d} — {region}",
            f"restaurante{i:02d}@connectongs.dev",
            SEED_PASSWORD,
            "RESTAURANTE",
            region,
        ))

    for i in range(1, N_ONGS + 1):
        region = REGIONS[(i - 1) % len(REGIONS)]
        specs.append((
            f"ONG Solidária {i:04d}",
            f"ong{i:04d}@connectongs.dev",
            SEED_PASSWORD,
            "ONG",
            region,
        ))

    return specs


def seed_users(conn: sqlite3.Connection) -> tuple:
    """
    Cria N_RESTAURANTS + N_ONGS usuários em paralelo.

    Etapa 1 (paralela, CPU): gera hashes bcrypt com Pool
    Etapa 2 (serial, I/O):   insere no SQLite via executemany + INSERT OR IGNORE

    Retorna (n_inseridos, n_já_existiam, tempo_total)
    """
    total = N_RESTAURANTS + N_ONGS
    specs = build_user_specs()

    n_workers = max(1, os.cpu_count() or 1)

    print(f"\n  Etapa 1/2 — Hashing bcrypt ({total} usuários, {n_workers} processos)...", flush=True)
    t0 = time.perf_counter()

    with multiprocessing.Pool(processes=n_workers) as pool:
        hashed_rows = pool.map(_hash_user, specs)

    t_hash = time.perf_counter() - t0
    print(f"  Hashing concluído em {t_hash:.2f}s  "
          f"({total/t_hash:.0f} hashes/s, {n_workers} processos paralelos)", flush=True)

    print(f"\n  Etapa 2/2 — Inserindo {total} usuários no banco (INSERT OR IGNORE)...", flush=True)
    t1 = time.perf_counter()

    cur = conn.cursor()
    # INSERT OR IGNORE — idempotente: não duplica em re-execuções
    cur.executemany(
        "INSERT OR IGNORE INTO users(name, email, password_hash, user_type, region) "
        "VALUES (?, ?, ?, ?, ?)",
        hashed_rows,
    )
    conn.commit()

    n_inserted = cur.rowcount   # linhas realmente inseridas (0 se já existiam)
    n_existing = total - n_inserted

    t_db = time.perf_counter() - t1
    print(f"  DB insert em {t_db:.3f}s", flush=True)

    return n_inserted, n_existing, t_hash + t_db


def seed_foods(conn: sqlite3.Connection):
    """
    Cria N_FOODS_PER_RESTAURANT alimentos por restaurante.
    Idempotente: verifica se já existem antes de inserir.
    """
    import random
    random.seed(42)   # reprodutível

    cur = conn.cursor()
    cur.execute("SELECT id, region FROM users WHERE user_type='RESTAURANTE' ORDER BY id")
    restaurants = cur.fetchall()

    if not restaurants:
        print("  [!] Nenhum restaurante encontrado — execute seed de usuários primeiro.", flush=True)
        return 0

    rows = []
    today = date.today()
    for rest in restaurants:
        for j in range(N_FOODS_PER_RESTAURANT):
            days = random.randint(1, 14)
            rows.append((
                rest['id'],
                random.choice(FOOD_NAMES),
                random.choice(FOOD_CATEGORIES),
                random.randint(5, 50),
                (today + timedelta(days=days)).isoformat(),
                rest['region'],
            ))

    cur.executemany(
        "INSERT OR IGNORE INTO foods(restaurant_id, name, category, quantity, expiry_date, region) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    return cur.rowcount


def print_status(conn: sqlite3.Connection):
    """Exibe estado atual do banco de dados."""
    counts = _current_counts(conn)
    print()
    print("  ┌──────────────────────────────────────────────┐")
    print("  │   Estado atual do banco de dados             │")
    print("  ├──────────────────────────────────────────────┤")
    print(f"  │   Restaurantes  : {counts.get('RESTAURANTE', 0):>6}                      │")
    print(f"  │   ONGs          : {counts.get('ONG', 0):>6}                      │")
    total_users = counts.get('RESTAURANTE', 0) + counts.get('ONG', 0)
    print(f"  │   Total usuários: {total_users:>6}                      │")
    print(f"  │   Alimentos     : {counts.get('FOODS', 0):>6}                      │")
    print("  └──────────────────────────────────────────────┘")
    print()
    if total_users > 0:
        print(f"  Credencial de acesso:")
        print(f"    ONGs         → ong0001@connectongs.dev até ong{N_ONGS:04d}@connectongs.dev")
        print(f"    Restaurantes → restaurante01@connectongs.dev até restaurante{N_RESTAURANTS:02d}@connectongs.dev")
        print(f"    Senha única  → {SEED_PASSWORD}")
    print()


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == '__main__':
    BANNER = r"""
  ╔══════════════════════════════════════════════════════════════════╗
  ║    C O N N E C T O N G S  —  Semeador de Banco de Dados        ║
  ║    1000 usuários  ·  multiprocessing.Pool  ·  bcrypt paralelo  ║
  ╚══════════════════════════════════════════════════════════════════╝
    """
    print(BANNER)
    print(f"  Banco     : {DB_PATH}")
    print(f"  Usuários  : {N_RESTAURANTS} restaurantes + {N_ONGS} ONGs = {N_RESTAURANTS + N_ONGS} total")
    print(f"  Alimentos : {N_FOODS_PER_RESTAURANT} por restaurante = {N_RESTAURANTS * N_FOODS_PER_RESTAURANT} total")
    print(f"  Senha     : {SEED_PASSWORD}  (bcrypt rounds={BCRYPT_ROUNDS} para velocidade)")
    print(f"  CPUs      : {os.cpu_count()} (paralelismo do hashing)")

    conn = _get_conn()
    _init_schema(conn)

    if '--status' in sys.argv:
        print_status(conn)
        conn.close()
        sys.exit(0)

    if '--reset' in sys.argv:
        print("\n  Apagando dados existentes (--reset)...")
        conn.execute("DELETE FROM audit_log")
        conn.execute("DELETE FROM notifications")
        conn.execute("DELETE FROM reservations")
        conn.execute("DELETE FROM foods")
        conn.execute("DELETE FROM users")
        conn.commit()
        print("  Tabelas limpas.")

    # Verifica o que já existe
    existing = _current_counts(conn)
    total_existing = existing.get('RESTAURANTE', 0) + existing.get('ONG', 0)

    if total_existing >= (N_RESTAURANTS + N_ONGS) and '--reset' not in sys.argv:
        print(f"\n  Banco já contém {total_existing} usuários — nada a fazer.")
        print("  Use --reset para recriar, ou --status para ver o estado atual.\n")
        print_status(conn)
        conn.close()
        sys.exit(0)

    # ── Seed de usuários ──────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    t_total = time.perf_counter()

    n_ins, n_skip, t_users = seed_users(conn)

    print(f"\n  Resultado seed de usuários:")
    print(f"    Inseridos   : {n_ins}")
    print(f"    Já existiam : {n_skip}")
    print(f"    Tempo total : {t_users:.2f}s")

    # ── Seed de alimentos ─────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"  Criando alimentos...", flush=True)
    t_food = time.perf_counter()
    n_foods = seed_foods(conn)
    t_food = time.perf_counter() - t_food
    print(f"  Alimentos criados: {n_foods} em {t_food:.3f}s")

    # ── Relatório final ───────────────────────────────────────────────────────
    t_total = time.perf_counter() - t_total
    print(f"\n{'─'*60}")
    print_status(conn)
    print(f"  Tempo total de seed: {t_total:.2f}s")
    print(f"  Banco pronto para testes de carga!")
    print(f"\n  Próximos passos:")
    print(f"    • Benchmark local  : python3 benchmark_runner.py")
    print(f"    • Benchmark Docker : docker compose up --build")
    print()

    conn.close()
