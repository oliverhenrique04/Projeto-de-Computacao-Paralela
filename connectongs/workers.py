"""
Processos daemon de background — executam de forma independente e desacoplada
do fluxo principal da aplicação.

ExpiryCheckerWorker: verifica periodicamente alimentos vencidos e dispara
notificações para os restaurantes afetados via fila.

Para o dev de distribuição: estes workers podem ser facilmente movidos para
containers separados — basta apontar DB_PATH para o mesmo banco (ou um
endpoint de API) e a fila de notificações para Redis/RabbitMQ.
"""
import os
import time
import multiprocessing

from . import foods, notifications, database as db
from . import logger as log


def expiry_checker_loop(stop_event: multiprocessing.Event,
                        interval: float = 5.0):
    """
    Processo background que roda em loop, expirando alimentos vencidos.

    Desacoplado: não bloqueia, não é chamado pelo fluxo principal.
    Comunica resultados via fila de notificações.
    """
    pid = os.getpid()
    log.worker(
        f"[ExpiryChecker PID {pid}] Iniciado — verificando a cada {interval}s"
    )

    cycle = 0
    while not stop_event.is_set():
        cycle += 1
        log.worker(f"[ExpiryChecker PID {pid}] Ciclo {cycle} — verificando validades...")

        expired_count = foods.expire_foods()

        if expired_count > 0:
            # Busca restaurantes com alimentos expirados recentemente
            with db.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT u.id AS uid, u.name AS uname, COUNT(f.id) AS qtd
                    FROM users u
                    JOIN foods f ON f.restaurant_id = u.id
                    WHERE f.status = 'EXPIRADO'
                      AND f.created_at >= datetime('now', '-10 minutes')
                    GROUP BY u.id
                """)
                affected = cur.fetchall()

            for row in affected:
                msg = (
                    f"AVISO AUTOMÁTICO: {row['qtd']} alimento(s) do restaurante "
                    f"'{row['uname']}' atingiram a data de validade e foram "
                    f"removidos da listagem automaticamente."
                )
                notifications.enqueue(row['uid'], msg)
                log.worker(
                    f"[ExpiryChecker PID {pid}] Notificação disparada "
                    f"→ restaurante ID {row['uid']}"
                )
        else:
            log.worker(
                f"[ExpiryChecker PID {pid}] Nenhum alimento expirado neste ciclo."
            )

        # Aguarda o intervalo (ou encerra se stop_event for sinalizado)
        stop_event.wait(timeout=interval)

    log.worker(f"[ExpiryChecker PID {pid}] Encerrado após {cycle} ciclo(s).")


def start_expiry_checker(stop_event: multiprocessing.Event,
                          interval: float = 5.0) -> multiprocessing.Process:
    """Factory: cria e inicia o processo ExpiryChecker."""
    proc = multiprocessing.Process(
        target=expiry_checker_loop,
        args=(stop_event, interval),
        name='ExpiryChecker',
        daemon=True
    )
    proc.start()
    log.worker(f"[ExpiryChecker] Processo iniciado — PID {proc.pid}")
    return proc


def start_notification_worker(queue: multiprocessing.Queue,
                               stop_event: multiprocessing.Event
                               ) -> multiprocessing.Process:
    """Factory: cria e inicia o processo NotificationWorker."""
    from . import notifications as notif
    proc = multiprocessing.Process(
        target=notif.notification_worker_loop,
        args=(queue, stop_event),
        name='NotificationWorker',
        daemon=True
    )
    proc.start()
    log.worker(f"[NotifWorker] Processo iniciado — PID {proc.pid}")
    return proc
