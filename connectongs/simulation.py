"""
Cenários de simulação de concorrência e paralelismo do CONNECTONGS.

Cada cenário demonstra um mecanismo diferente do multiprocessing:

  Cenário 1 — Logins simultâneos       → multiprocessing.Semaphore + Pool
  Cenário 2 — Corrida por alimento     → multiprocessing.Lock + UNIQUE INDEX
  Cenário 3 — Notificações em lote     → multiprocessing.Pool (dispatch paralelo)
  Cenário 4 — Fila + worker assíncrono → multiprocessing.Queue + Process
  Cenário 5 — Worker de expiração      → multiprocessing.Process daemon
"""
import os
import time
import multiprocessing
from datetime import date, timedelta

from . import auth, foods, reservations, notifications, workers
from . import database as db
from . import logger as log


# ─── Funções TOP-LEVEL para Pool.map (precisam ser picklables) ────────────────

def _login_worker(args):
    """Executada por cada processo do Pool no cenário de login simultâneo."""
    email, password = args
    return auth.login(email, password)


def _reserve_worker(args):
    """Executada por cada processo do Pool no cenário de corrida por alimento."""
    ong_id, food_id, ong_name = args
    return reservations.reserve_food(ong_id, food_id, ong_name)


# ─── Inicializadores de Pool ──────────────────────────────────────────────────

def _init_semaphore(sem):
    auth.set_login_semaphore(sem)


def _init_lock(lock):
    reservations.set_reservation_lock(lock)


# ─── Seed de dados de teste ───────────────────────────────────────────────────

def seed_users() -> list:
    """Cria usuários de teste idempotentemente (não duplica em re-execuções)."""
    users = []

    # 1 Restaurante
    try:
        u = auth.register(
            "Restaurante Central Brasília", "restaurante@connectongs.dev",
            "Senha@123", "RESTAURANTE", "Plano Piloto"
        )
    except ValueError:
        with db.cursor() as cur:
            cur.execute(
                "SELECT * FROM users WHERE email='restaurante@connectongs.dev'"
            )
            u = dict(cur.fetchone())
    users.append(u)

    # 10 ONGs
    for i in range(1, 11):
        email = f"ong{i:02d}@connectongs.dev"
        try:
            u = auth.register(
                f"ONG Solidária {i:02d}", email,
                "Senha@123", "ONG", "Plano Piloto"
            )
        except ValueError:
            with db.cursor() as cur:
                cur.execute("SELECT * FROM users WHERE email=?", (email,))
                u = dict(cur.fetchone())
        users.append(u)

    return users


def seed_food(restaurant_id: int, name: str = None,
              days_ahead: int = 5) -> dict:
    """Cadastra um alimento de teste com validade futura."""
    import random
    exp = (date.today() + timedelta(days=days_ahead)).isoformat()
    name = name or f"Alimento-{random.randint(1000, 9999)}"
    return foods.add_food(
        restaurant_id, name, "Refeição", 10, exp, "Plano Piloto"
    )


# ─── CENÁRIO 1: Logins simultâneos com Semáforo ───────────────────────────────

def cenario_logins_simultaneos(users: list, n_concurrent: int = None,
                               sem_limit: int = 5):
    ongs = [u for u in users if u['user_type'] == 'ONG']
    if n_concurrent:
        ongs = ongs[:n_concurrent]
    n = len(ongs)

    log.sep("CENÁRIO 1 — Login Simultâneo (Semáforo)")
    log.sim(f"{n} usuários tentam logar ao mesmo tempo.")
    log.sim(f"Semáforo = {sem_limit} → no máximo {sem_limit} autenticações ocorrem em paralelo.")

    semaphore = multiprocessing.Semaphore(sem_limit)
    # Usa a senha do dict se existir (banco seed usa Teste@1234, seed_users usa Senha@123)
    credentials = [(u['email'], u.get('_password', 'Senha@123')) for u in ongs]

    # Limita processos do pool a no máximo 100 para não estourar memória no Windows
    pool_size = min(len(credentials), 100)
    log.sim(f"Disparando {len(credentials)} logins simultâneos via Pool ({pool_size} workers)...")
    inicio = time.time()

    with multiprocessing.Pool(
        processes=pool_size,
        initializer=_init_semaphore,
        initargs=(semaphore,)
    ) as pool:
        results = pool.map(_login_worker, credentials)

    elapsed = time.time() - inicio
    ok = sum(1 for r in results if r is not None)

    log.sep()
    log.success(
        f"Resultado: {ok}/{len(credentials)} logins OK | "
        f"Tempo total: {elapsed:.2f}s | "
        f"Semáforo limitou a {sem_limit} simultâneos"
    )


# ─── CENÁRIO 2: Corrida por alimento (race condition) ─────────────────────────

def cenario_corrida_alimento(users: list, restaurant_id: int,
                             n_concurrent: int = None):
    ongs_all = [u for u in users if u['user_type'] == 'ONG']
    n = min(n_concurrent or 8, len(ongs_all))
    n = max(2, n)  # minimo 2 para ter sentido

    log.sep(f"CENÁRIO 2 — Race Condition: {n} ONGs × 1 Alimento")
    log.sim(f"{n} ONGs tentam reservar o MESMO alimento simultaneamente.")
    log.sim("Apenas 1 deve ser confirmada. Demais recebem 409 Conflict.")

    food = seed_food(restaurant_id, name="Pizza Margherita (última unidade!)")
    log.sim(
        f"Alimento criado: '{food['name']}' (ID {food['id']}) — "
        f"Status: DISPONIVEL"
    )

    lock = multiprocessing.Lock()
    ongs = ongs_all[:n]
    args = [(u['id'], food['id'], u['name']) for u in ongs]

    log.sim("LARGADA! Todas as 8 ONGs tentando ao mesmo tempo...")
    inicio = time.time()

    with multiprocessing.Pool(
        processes=n,
        initializer=_init_lock,
        initargs=(lock,)
    ) as pool:
        results = pool.map(_reserve_worker, args)

    elapsed = time.time() - inicio
    winners = [r for r in results if r['success']]
    losers  = [r for r in results if not r['success']]

    log.sep()
    log.success(f"VENCEDOR(ES): {len(winners)} reserva confirmada")
    log.conflict(f"CONFLITOS:    {len(losers)} ONGs receberam 409 Conflict")
    consistente = "SIM ✓" if len(winners) == 1 else "FALHA ✗"
    log.sim(
        f"Consistência (exatamente 1 vencedor): {consistente} | "
        f"Tempo: {elapsed:.2f}s"
    )
    return food


# ─── CENÁRIO 3: Notificações em lote via Pool ─────────────────────────────────

def cenario_notificacoes_lote(users: list, food: dict):
    log.sep("CENÁRIO 3 — Notificações em Paralelo (Pool.map)")
    log.sim("Restaurante cadastra alimento → sistema notifica todas as ONGs.")
    log.sim("Cada worker do Pool envia para uma ONG diferente simultaneamente.")

    ongs    = [u for u in users if u['user_type'] == 'ONG']
    ong_ids = [u['id'] for u in ongs]
    message = (
        f"NOVA DOAÇÃO DISPONÍVEL: '{food['name']}' no Plano Piloto. "
        f"Faça login e reserve agora!"
    )

    log.sim(
        f"Disparando {len(ong_ids)} notificações para "
        f"{len(ong_ids)} ONGs em PARALELO..."
    )
    inicio = time.time()
    notifications.dispatch_bulk(ong_ids, message)
    elapsed = time.time() - inicio

    log.sep()
    log.success(
        f"Todas as {len(ong_ids)} notificações entregues em {elapsed:.2f}s "
        f"(em paralelo — não sequencialmente!)"
    )


# ─── CENÁRIO 4: Fila de notificações + worker assíncrono ─────────────────────

def cenario_fila_notificacoes(users: list):
    log.sep("CENÁRIO 4 — Fila de Notificações + Worker Process (desacoplado)")
    log.sim("Fluxo principal enfileira eventos e retorna IMEDIATAMENTE.")
    log.sim("NotificationWorker (processo separado) consome a fila em background.")

    queue    = multiprocessing.Queue()
    stop_ev  = multiprocessing.Event()

    # Inicia o worker em processo separado
    worker_proc = workers.start_notification_worker(queue, stop_ev)
    log.sim(f"Worker iniciado — PID {worker_proc.pid} (processo independente)")

    # Conecta a fila global
    notifications.set_notification_queue(queue)

    ongs = [u for u in users if u['user_type'] == 'ONG']
    eventos = [
        "Bem-vindo ao CONNECTONGS! Sua conta foi confirmada.",
        "Nova doação disponível na sua região: Marmitas Executivas.",
        "Lembrete: sua reserva de 'Saladas Mistas' expira em 2 horas.",
        "Avaliação recebida: Restaurante Central deu ⭐⭐⭐⭐⭐",
        "ALERTA: Alimento 'Sopa Minestrone' marcado como retirado com sucesso.",
    ]

    log.sim(f"Enfileirando {len(eventos)} eventos (não-bloqueante)...")
    for ong, msg in zip(ongs[:5], eventos):
        notifications.enqueue(ong['id'], msg)
        log.info(
            f"[Main PID {os.getpid()}] Evento enfileirado. "
            f"Fluxo principal continua sem bloquear..."
        )
        time.sleep(0.08)

    log.sim(
        f"Fluxo principal livre para continuar. "
        f"Worker PID {worker_proc.pid} processa em background..."
    )
    time.sleep(1.5)   # deixa o worker processar a fila

    # Encerramento gracioso
    stop_ev.set()
    worker_proc.join(timeout=3)
    notifications.set_notification_queue(None)

    log.sep()
    log.success("Fila drenada. Worker encerrado. Fluxo principal nunca bloqueou.")


# ─── CENÁRIO 5: Worker de expiração automática em background ─────────────────

def cenario_expiracao_automatica(restaurant_id: int):
    log.sep("CENÁRIO 5 — ExpiryChecker: Worker de Expiração em Background")
    log.sim("Alimentos vencidos são detectados e removidos automaticamente.")
    log.sim("O worker roda em processo separado — independente de requisições.")

    # Insere alimentos com data vencida para simular expiração
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO foods(restaurant_id,name,category,quantity,"
            "expiry_date,region) VALUES(?,?,?,?,?,?)",
            (restaurant_id, "Sopa de Legumes (vencida)", "Sopa",
             5, yesterday, "Plano Piloto")
        )
        cur.execute(
            "INSERT INTO foods(restaurant_id,name,category,quantity,"
            "expiry_date,region) VALUES(?,?,?,?,?,?)",
            (restaurant_id, "Frango Grelhado (vencido)", "Proteína",
             3, yesterday, "Plano Piloto")
        )

    log.sim("2 alimentos com validade vencida inseridos no banco.")
    log.sim("Iniciando ExpiryChecker (verifica a cada 2s)...")

    stop_ev = multiprocessing.Event()
    checker = workers.start_expiry_checker(stop_ev, interval=2.0)

    time.sleep(3.5)   # 1-2 ciclos de verificação

    stop_ev.set()
    checker.join(timeout=3)

    # Confirma resultado no banco
    with db.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM foods WHERE status='EXPIRADO'"
        )
        total_exp = cur.fetchone()['c']

    log.sep()
    log.success(f"Total de alimentos com status EXPIRADO no banco: {total_exp}")


# ─── Relatório final ──────────────────────────────────────────────────────────

def print_report():
    log.sep("RELATÓRIO FINAL — Estado do Banco de Dados")

    with db.cursor() as cur:
        cur.execute(
            "SELECT user_type, COUNT(*) AS c FROM users GROUP BY user_type"
        )
        users_by_type = {r['user_type']: r['c'] for r in cur.fetchall()}

        cur.execute(
            "SELECT status, COUNT(*) AS c FROM foods GROUP BY status"
        )
        foods_by_status = {r['status']: r['c'] for r in cur.fetchall()}

        cur.execute(
            "SELECT status, COUNT(*) AS c FROM reservations GROUP BY status"
        )
        res_by_status = {r['status']: r['c'] for r in cur.fetchall()}

        cur.execute("SELECT COUNT(*) AS c FROM notifications")
        n_notif = cur.fetchone()['c']

        cur.execute(
            "SELECT event_type, COUNT(*) AS c FROM audit_log GROUP BY event_type"
        )
        audit_by_type = {r['event_type']: r['c'] for r in cur.fetchall()}

    print(f"\n  Usuários    → {dict(users_by_type)}")
    print(f"  Alimentos   → {dict(foods_by_status)}")
    print(f"  Reservas    → {dict(res_by_status)}")
    print(f"  Notificações → {n_notif} registradas no banco")
    print(f"  Auditoria   → {dict(audit_by_type)}")
    log.sep()
