"""
Gestão de alimentos: cadastro, busca e expiração automática.
"""
import os
from datetime import date, datetime

from . import database as db
from . import logger as log


def add_food(restaurant_id: int, name: str, category: str,
             quantity: int, expiry_date: str, region: str) -> dict:
    """Cadastra alimento. expiry_date formato 'YYYY-MM-DD'. RN01: data deve ser futura."""
    exp = datetime.strptime(expiry_date, '%Y-%m-%d').date()
    if exp <= date.today():
        raise ValueError("Data de validade deve ser posterior à data atual (RN01).")

    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO foods(restaurant_id,name,category,quantity,expiry_date,region) "
            "VALUES(?,?,?,?,?,?)",
            (restaurant_id, name, category, quantity, expiry_date, region)
        )
        food_id = cur.lastrowid

    food = {
        'id': food_id, 'restaurant_id': restaurant_id, 'name': name,
        'category': category, 'quantity': quantity,
        'expiry_date': expiry_date, 'region': region, 'status': 'DISPONIVEL'
    }
    db.audit('FOOD_ADDED', restaurant_id,
             f"food_id={food_id},name={name},region={region}")
    log.success(f"Alimento cadastrado: '{name}' ID {food_id} | Validade: {expiry_date} | Região: {region}")
    return food


def get_food(food_id: int):
    with db.cursor() as cur:
        cur.execute("SELECT * FROM foods WHERE id=?", (food_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def list_foods(region: str = None, category: str = None,
               status: str = 'DISPONIVEL') -> list:
    query  = "SELECT * FROM foods WHERE status=?"
    params = [status]
    if region:
        query += " AND region=?"
        params.append(region)
    if category:
        query += " AND category=?"
        params.append(category)

    with db.cursor() as cur:
        cur.execute(query, params)
        return [dict(r) for r in cur.fetchall()]


def expire_foods() -> int:
    """
    Marca como EXPIRADO alimentos com validade vencida.
    Chamado pelo ExpiryChecker em background — desacoplado do fluxo principal.
    Retorna a quantidade expirada nesta execução.
    """
    today = date.today().isoformat()
    with db.cursor() as cur:
        cur.execute(
            "UPDATE foods SET status='EXPIRADO' "
            "WHERE status='DISPONIVEL' AND expiry_date < ?",
            (today,)
        )
        count = cur.rowcount

    if count > 0:
        log.warning(f"[ExpiryChecker PID {os.getpid()}] {count} alimento(s) expirado(s) marcados.")
        db.audit('EXPIRY_RUN', None, f"expired={count},pid={os.getpid()}")

    return count


def cancel_food(food_id: int, restaurant_id: int) -> bool:
    with db.cursor() as cur:
        cur.execute(
            "UPDATE foods SET status='CANCELADO' WHERE id=? AND restaurant_id=?",
            (food_id, restaurant_id)
        )
        ok = cur.rowcount > 0
    if ok:
        db.audit('FOOD_CANCELLED', restaurant_id, f"food_id={food_id}")
    return ok
