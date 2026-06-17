"""
API HTTP do CONNECTONGS para clientes distribuídos (Docker workers).

Usa ThreadingHTTPServer da stdlib — sem dependências externas.
Cada requisição é tratada em uma thread separada; o SQLite em WAL mode
e os primitivos de sincronização (Semaphore, Lock) garantem consistência
mesmo com múltiplos clientes simultâneos.

Endpoints:
  GET  /health             — health check (retorna pid e uptime)
  GET  /stats              — contagem de registros no banco
  POST /auth/register      — cadastro de usuário
  POST /auth/login         — autenticação com Semaphore (throttle)
  POST /foods/add          — cadastrar alimento
  POST /foods/reserve      — reservar alimento com Lock (seção crítica)
"""
import json
import os
import sys
import time
import multiprocessing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import date, timedelta

# Acrescenta a raiz do projeto ao path para imports locais funcio­narem
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from . import auth, foods, reservations, database as db
from . import logger as log

_SERVER_START = time.time()
_login_semaphore: multiprocessing.Semaphore = None   # type: ignore
_reservation_lock: multiprocessing.Lock = None        # type: ignore


def _ensure_primitives():
    """Inicializa Semaphore e Lock compartilhados pelo servidor."""
    global _login_semaphore, _reservation_lock
    if _login_semaphore is None:
        _login_semaphore = multiprocessing.Semaphore(5)
        auth.set_login_semaphore(_login_semaphore)
        log.info("[API] Semaphore de login criado (limite=5 simultâneos)")
    if _reservation_lock is None:
        _reservation_lock = multiprocessing.Lock()
        reservations.set_reservation_lock(_reservation_lock)
        log.info("[API] Lock de reserva criado")


# ─── Handler HTTP ──────────────────────────────────────────────────────────────

class ConnectONGSHandler(BaseHTTPRequestHandler):
    """Processa cada requisição HTTP em sua própria thread (ThreadingHTTPServer)."""

    def log_message(self, fmt, *args):
        # Substitui log padrão do HTTPServer pelo logger colorido do projeto
        log.info(f"[API] {self.client_address[0]} {fmt % args}")

    # ── helpers ──────────────────────────────────────────────────────────────

    def _read_body(self) -> dict:
        length = int(self.headers.get('Content-Length', 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw)

    def _send(self, code: int, payload: dict):
        body = json.dumps(payload, default=str).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ── GET ───────────────────────────────────────────────────────────────────

    def do_GET(self):
        if self.path == '/health':
            self._send(200, {
                'status': 'ok',
                'pid': os.getpid(),
                'uptime_s': round(time.time() - _SERVER_START, 2),
            })

        elif self.path == '/stats':
            with db.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS c FROM users")
                users_c = cur.fetchone()['c']
                cur.execute(
                    "SELECT COUNT(*) AS c FROM foods WHERE status='DISPONIVEL'"
                )
                foods_c = cur.fetchone()['c']
                cur.execute("SELECT COUNT(*) AS c FROM reservations")
                res_c = cur.fetchone()['c']
                cur.execute("SELECT COUNT(*) AS c FROM notifications")
                notif_c = cur.fetchone()['c']
            self._send(200, {
                'users': users_c,
                'available_foods': foods_c,
                'reservations': res_c,
                'notifications': notif_c,
            })

        elif self.path.startswith('/users/ongs'):
            # GET /users/ongs?limit=250&offset=0
            # Retorna fatia de ONGs para os workers distribuírem carga
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            limit  = int(qs.get('limit',  ['250'])[0])
            offset = int(qs.get('offset', ['0'])[0])
            with db.cursor() as cur:
                cur.execute(
                    "SELECT id, name, email FROM users "
                    "WHERE user_type='ONG' ORDER BY id LIMIT ? OFFSET ?",
                    (limit, offset)
                )
                rows = [dict(r) for r in cur.fetchall()]
                cur.execute("SELECT COUNT(*) AS c FROM users WHERE user_type='ONG'")
                total = cur.fetchone()['c']
            self._send(200, {'ongs': rows, 'total': total,
                             'limit': limit, 'offset': offset})

        elif self.path.startswith('/users/restaurants'):
            # GET /users/restaurants?limit=50&offset=0
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            limit  = int(qs.get('limit',  ['50'])[0])
            offset = int(qs.get('offset', ['0'])[0])
            with db.cursor() as cur:
                cur.execute(
                    "SELECT id, name, email, region FROM users "
                    "WHERE user_type='RESTAURANTE' ORDER BY id LIMIT ? OFFSET ?",
                    (limit, offset)
                )
                rows = [dict(r) for r in cur.fetchall()]
            self._send(200, {'restaurants': rows})

        elif self.path.startswith('/foods/available'):
            # GET /foods/available?limit=20&offset=0
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            limit  = int(qs.get('limit',  ['20'])[0])
            offset = int(qs.get('offset', ['0'])[0])
            with db.cursor() as cur:
                cur.execute(
                    "SELECT id, name, restaurant_id FROM foods "
                    "WHERE status='DISPONIVEL' ORDER BY id LIMIT ? OFFSET ?",
                    (limit, offset)
                )
                rows = [dict(r) for r in cur.fetchall()]
            self._send(200, {'foods': rows})

        else:
            self._send(404, {'error': 'endpoint not found'})

    # ── POST ──────────────────────────────────────────────────────────────────

    def do_POST(self):
        try:
            body = self._read_body()
            t0 = time.perf_counter()

            # ── /auth/register ────────────────────────────────────────────────
            if self.path == '/auth/register':
                user = auth.register(
                    body['name'],
                    body['email'],
                    body['password'],
                    body.get('user_type', 'ONG'),
                    body.get('region', 'Plano Piloto'),
                )
                elapsed = (time.perf_counter() - t0) * 1000
                self._send(201, {'user_id': user['id'], 'elapsed_ms': elapsed})

            # ── /auth/login ───────────────────────────────────────────────────
            elif self.path == '/auth/login':
                user = auth.login(body['email'], body['password'])
                elapsed = (time.perf_counter() - t0) * 1000
                if user:
                    self._send(200, {
                        'success': True,
                        'user_id': user['id'],
                        'user_type': user['user_type'],
                        'elapsed_ms': elapsed,
                    })
                else:
                    self._send(401, {'success': False, 'elapsed_ms': elapsed})

            # ── /foods/add ────────────────────────────────────────────────────
            elif self.path == '/foods/add':
                exp = (
                    date.fromisoformat(body['expiry_date'])
                    if 'expiry_date' in body
                    else date.today() + timedelta(days=5)
                ).isoformat()
                food = foods.add_food(
                    int(body['restaurant_id']),
                    body.get('name', 'Alimento-API'),
                    body.get('category', 'Refeição'),
                    int(body.get('quantity', 10)),
                    exp,
                    body.get('region', 'Plano Piloto'),
                )
                elapsed = (time.perf_counter() - t0) * 1000
                self._send(201, {'food_id': food['id'], 'elapsed_ms': elapsed})

            # ── /foods/reserve ────────────────────────────────────────────────
            elif self.path == '/foods/reserve':
                result = reservations.reserve_food(
                    int(body['ong_id']),
                    int(body['food_id']),
                    body.get('ong_name', 'ONG-Docker'),
                )
                elapsed = (time.perf_counter() - t0) * 1000
                http_code = 200 if result['success'] else 409
                result['elapsed_ms'] = elapsed
                self._send(http_code, result)

            else:
                self._send(404, {'error': f"endpoint '{self.path}' não existe"})

        except KeyError as exc:
            self._send(400, {'error': f'campo obrigatório ausente: {exc}'})
        except ValueError as exc:
            self._send(422, {'error': str(exc)})
        except Exception as exc:
            import traceback
            log.error(f"[API] Erro interno: {exc}")
            self._send(500, {
                'error': str(exc),
                'trace': traceback.format_exc(),
            })


# ─── Entry point ──────────────────────────────────────────────────────────────

def run_server(host: str = '0.0.0.0', port: int = 8080):
    """Inicializa o banco, garante primitivos de sincronização e sobe o servidor."""
    db.init_db()
    _ensure_primitives()

    server = ThreadingHTTPServer((host, port), ConnectONGSHandler)
    log.success(
        f"[API] CONNECTONGS API Server — {host}:{port} | "
        f"PID {os.getpid()} | ThreadingHTTPServer ativo"
    )
    log.info("[API] GET  /health  /stats  /users/ongs  /users/restaurants  /foods/available")
    log.info("[API] POST /auth/register  /auth/login  /foods/add  /foods/reserve")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.warning("[API] Servidor encerrado via KeyboardInterrupt.")


if __name__ == '__main__':
    PORT = int(os.environ.get('API_PORT', '8080'))
    HOST = os.environ.get('API_HOST', '0.0.0.0')
    run_server(host=HOST, port=PORT)
