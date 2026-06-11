"""
Sistema de notificações com dois padrões paralelos:

  1. Pool.map (dispatch em lote)   — N workers processam N notificações
                                    simultaneamente. Cada processo do pool
                                    envia para uma ONG diferente em paralelo.

  2. Queue + Process (fila assíncrona) — O fluxo principal enfileira eventos
                                    sem bloquear. Um Process separado consome
                                    a fila em background e persiste no banco.

O segundo padrão é o requisito obrigatório "fila + worker" da avaliação.
"""
import os
import time
import multiprocessing

from . import database as db
from . import logger as log

# Fila global — configurada pelo processo principal
_notification_queue = None


def set_notification_queue(q):
    global _notification_queue
    _notification_queue = q


# ─── Persistência ─────────────────────────────────────────────────────────────

def _persist(user_id: int, message: str):
    """Salva a notificação no banco. Chamado por workers — cada um com sua conexão."""
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO notifications(user_id, message) VALUES(?,?)",
            (user_id, message)
        )


# ─── Padrão 1: Pool (dispatch em lote paralelo) ───────────────────────────────

def _pool_dispatch(args):
    """
    Função top-level para Pool.map.
    Cada worker do pool executa esta função para uma ONG diferente.
    Precisa ser top-level (não closure) para ser picklable no multiprocessing.
    """
    user_id, message = args
    pid = os.getpid()
    log.worker(
        f"[Pool Worker PID {pid}] → Usuário {user_id}: '{message[:55]}...'"
    )
    time.sleep(0.04)   # simula latência de envio (push/e-mail)
    _persist(user_id, message)
    return user_id


def dispatch_bulk(ong_ids: list, message: str) -> list:
    """
    Envia notificações para múltiplas ONGs em paralelo via Pool.
    Cada processo do pool é um worker independente com seu próprio PID.
    """
    if not ong_ids:
        return []

    n_workers = min(len(ong_ids), multiprocessing.cpu_count())
    log.sim(
        f"[BulkNotify] Disparando {len(ong_ids)} notificações "
        f"via Pool ({n_workers} workers em paralelo)..."
    )

    args = [(uid, message) for uid in ong_ids]

    with multiprocessing.Pool(processes=n_workers) as pool:
        delivered = pool.map(_pool_dispatch, args)

    log.success(f"[BulkNotify] {len(delivered)} notificações entregues em paralelo.")
    return delivered


# ─── Padrão 2: Queue assíncrona (enfileira e retorna imediatamente) ───────────

def enqueue(user_id: int, message: str):
    """
    Coloca notificação na fila — não bloqueia o chamador.
    O NotificationWorker (processo separado) a consome de forma assíncrona.
    """
    if _notification_queue is not None:
        _notification_queue.put({'user_id': user_id, 'message': message})
        log.info(
            f"[Main PID {os.getpid()}] Notificação enfileirada "
            f"→ usuário {user_id}: '{message[:50]}'"
        )
    else:
        # Fallback: persiste diretamente se worker não estiver rodando
        _persist(user_id, message)


def notification_worker_loop(queue: multiprocessing.Queue,
                              stop_event: multiprocessing.Event):
    """
    Processo worker dedicado que consome a fila de notificações.
    Roda em um Process separado — totalmente desacoplado do fluxo principal.
    O fluxo principal continua sem esperar este processo terminar.
    """
    pid = os.getpid()
    log.worker(f"[NotifWorker PID {pid}] Iniciado — aguardando mensagens na fila...")

    while not stop_event.is_set():
        try:
            item = queue.get(timeout=0.5)

            if item is None:   # sentinel de encerramento gracioso
                log.worker(f"[NotifWorker PID {pid}] Sentinel recebido — encerrando.")
                break

            log.worker(
                f"[NotifWorker PID {pid}] Processando → "
                f"Usuário {item['user_id']}: '{item['message'][:55]}'"
            )
            time.sleep(0.02)   # simula I/O assíncrono
            _persist(item['user_id'], item['message'])
            log.worker(f"[NotifWorker PID {pid}] ✓ Persistida no banco.")

        except Exception:
            # queue.get timeout — normal durante idle, continua o loop
            pass

    log.worker(f"[NotifWorker PID {pid}] Encerrado.")
