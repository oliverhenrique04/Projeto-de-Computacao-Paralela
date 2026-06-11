"""
Reserva de alimentos com dupla proteção contra race condition:

  1. multiprocessing.Lock  — garante que apenas 1 processo por vez entre na
                             seção crítica (check + insert). Evita conflitos
                             desnecessários chegando ao banco.

  2. UNIQUE INDEX no banco — última linha de defesa. Se dois processos
                             ultrapassarem o lock ao mesmo tempo (ex: em deploy
                             distribuído com workers em containers diferentes),
                             o banco rejeita a segunda inserção com IntegrityError
                             e retorna HTTP 409 Conflict.

Esta dupla proteção é o padrão correto para sistemas distribuídos onde o Lock
de memória compartilhada não é suficiente (cada container tem seu próprio heap).
"""
import os
import sqlite3
import multiprocessing

from . import database as db
from . import logger as log

# Configurado pelo processo principal antes de criar o Pool
_reservation_lock = None


def set_reservation_lock(lock):
    global _reservation_lock
    _reservation_lock = lock


def reserve_food(ong_id: int, food_id: int, ong_name: str = '') -> dict:
    """
    Tenta reservar um alimento para uma ONG.
    Retorna {'success': bool, 'reservation_id': int|None, 'error': str|None}
    """
    pid  = os.getpid()
    tag  = f"{ong_name or f'ONG {ong_id}'} [PID {pid}]"

    log.info(f"{tag} → tentando reservar alimento ID {food_id}")

    if _reservation_lock is not None:
        log.lock_log(f"{tag} aguardando Lock de reserva...")
        _reservation_lock.acquire()
        log.lock_log(f"{tag} LOCK ADQUIRIDO")

    try:
        with db.cursor() as cur:
            # Verifica disponibilidade dentro da seção crítica
            cur.execute(
                "SELECT id, name FROM foods WHERE id=? AND status='DISPONIVEL'",
                (food_id,)
            )
            food_row = cur.fetchone()

            if not food_row:
                log.conflict(
                    f"{tag} ✗ CONFLITO 409 — Alimento {food_id} não disponível!"
                )
                db.audit('RESERVATION_CONFLICT', ong_id,
                         f"food_id={food_id},reason=not_available,pid={pid}")
                return {
                    'success': False,
                    'reservation_id': None,
                    'error': 'Este item já foi reservado por outra instituição (409 Conflict)'
                }

            try:
                cur.execute(
                    "INSERT INTO reservations(food_id, ong_id) VALUES(?,?)",
                    (food_id, ong_id)
                )
                reservation_id = cur.lastrowid

                # Atualiza status do alimento atomicamente na mesma transação
                cur.execute(
                    "UPDATE foods SET status='RESERVADO' WHERE id=?",
                    (food_id,)
                )

            except sqlite3.IntegrityError as e:
                # UNIQUE INDEX disparou — proteção do banco funcionou
                log.conflict(
                    f"{tag} ✗ CONFLITO 409 (IntegrityError) — {e}"
                )
                db.audit('RESERVATION_CONFLICT', ong_id,
                         f"food_id={food_id},reason=db_constraint,pid={pid}")
                return {
                    'success': False,
                    'reservation_id': None,
                    'error': 'Este item já foi reservado por outra instituição (409 Conflict)'
                }

        db.audit('RESERVATION_OK', ong_id,
                 f"food_id={food_id},reservation_id={reservation_id},pid={pid}")
        log.success(
            f"{tag} ✓ RESERVA CONFIRMADA — ID {reservation_id} | Alimento: '{food_row['name']}'"
        )
        return {'success': True, 'reservation_id': reservation_id, 'error': None}

    finally:
        if _reservation_lock is not None:
            _reservation_lock.release()
            log.lock_log(f"{tag} Lock LIBERADO")


def cancel_reservation(reservation_id: int, ong_id: int) -> bool:
    """Cancela reserva e devolve alimento ao status DISPONIVEL (FA02)."""
    with db.cursor() as cur:
        cur.execute(
            "SELECT food_id FROM reservations WHERE id=? AND ong_id=? AND status='PENDENTE'",
            (reservation_id, ong_id)
        )
        row = cur.fetchone()
        if not row:
            return False

        cur.execute(
            "UPDATE reservations SET status='CANCELADO' WHERE id=?",
            (reservation_id,)
        )
        cur.execute(
            "UPDATE foods SET status='DISPONIVEL' WHERE id=?",
            (row['food_id'],)
        )

    db.audit('RESERVATION_CANCELLED', ong_id,
             f"reservation_id={reservation_id},food_id={row['food_id']}")
    log.info(f"Reserva {reservation_id} cancelada — alimento {row['food_id']} voltou a DISPONIVEL")
    return True


def complete_reservation(reservation_id: int, ong_id: int) -> bool:
    """Marca reserva como concluída após retirada do alimento."""
    with db.cursor() as cur:
        cur.execute(
            "UPDATE reservations SET status='CONCLUIDO' "
            "WHERE id=? AND ong_id=? AND status='PENDENTE'",
            (reservation_id, ong_id)
        )
        ok = cur.rowcount > 0

    if ok:
        db.audit('RESERVATION_COMPLETED', ong_id, f"reservation_id={reservation_id}")
        log.success(f"Reserva {reservation_id} concluída.")
    return ok
