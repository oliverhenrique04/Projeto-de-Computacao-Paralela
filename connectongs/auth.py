"""
Autenticação de usuários com controle de concorrência via Semaphore.

O Semaphore limita quantos processos podem executar o login ao mesmo tempo,
simulando um throttle real de autenticação sob alta carga.
"""
import os
import time
import multiprocessing
import bcrypt

from . import database as db
from . import logger as log

# Global configurado pelo processo principal antes de criar o Pool
_login_semaphore = None


def set_login_semaphore(sem):
    global _login_semaphore
    _login_semaphore = sem


def register(name: str, email: str, password: str,
             user_type: str, region: str) -> dict:
    if user_type not in ('RESTAURANTE', 'ONG', 'ADMIN'):
        raise ValueError(f"Tipo inválido: {user_type}")

    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    with db.cursor() as cur:
        try:
            cur.execute(
                "INSERT INTO users(name,email,password_hash,user_type,region) "
                "VALUES(?,?,?,?,?)",
                (name, email, pw_hash, user_type, region)
            )
            user_id = cur.lastrowid
        except Exception as e:
            if 'UNIQUE' in str(e):
                raise ValueError(f"Email '{email}' já cadastrado.")
            raise

    db.audit('REGISTER', user_id, f"type={user_type},region={region}")
    log.success(f"Cadastro: {name} ({user_type}) — ID {user_id}")
    return {'id': user_id, 'name': name, 'email': email,
            'user_type': user_type, 'region': region}


def login(email: str, password: str):
    """
    Login com Semaphore: máximo N logins rodando em paralelo.
    Processos excedentes aguardam na fila do semáforo.
    """
    pid = os.getpid()

    if _login_semaphore is not None:
        log.lock_log(f"PID {pid} aguardando slot de login (semáforo)...")
        _login_semaphore.acquire()
        log.lock_log(f"PID {pid} slot de login ADQUIRIDO.")

    try:
        # bcrypt é intencionalmente lento — simula carga real de autenticação
        time.sleep(0.05)

        with db.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE email=?", (email,))
            row = cur.fetchone()

        if row and bcrypt.checkpw(password.encode(), row['password_hash'].encode()):
            user = dict(row)
            db.audit('LOGIN', user['id'], f"pid={pid}")
            log.success(f"Login OK → {user['name']} ({user['user_type']}) [PID {pid}]")
            return user

        log.warning(f"Falha no login: '{email}' [PID {pid}]")
        db.audit('LOGIN_FAIL', None, f"email={email},pid={pid}")
        return None

    finally:
        if _login_semaphore is not None:
            _login_semaphore.release()
            log.lock_log(f"PID {pid} slot de login LIBERADO.")


def get_all_ongs() -> list:
    with db.cursor() as cur:
        cur.execute(
            "SELECT id, name, email, region FROM users WHERE user_type='ONG'"
        )
        return [dict(r) for r in cur.fetchall()]


def get_ongs_by_region(region: str) -> list:
    with db.cursor() as cur:
        cur.execute(
            "SELECT id, name, email FROM users WHERE user_type='ONG' AND region=?",
            (region,)
        )
        return [dict(r) for r in cur.fetchall()]
