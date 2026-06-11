import os
from datetime import datetime

class _C:
    RESET   = '\033[0m'
    BOLD    = '\033[1m'
    RED     = '\033[91m'
    GREEN   = '\033[92m'
    YELLOW  = '\033[93m'
    BLUE    = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN    = '\033[96m'

_COLORS = {
    'INFO'    : _C.CYAN,
    'SUCCESS' : _C.GREEN,
    'WARNING' : _C.YELLOW,
    'ERROR'   : _C.BOLD + _C.RED,
    'WORKER'  : _C.MAGENTA,
    'CONFLICT': _C.BOLD + _C.RED,
    'LOCK'    : _C.BOLD + _C.YELLOW,
    'SIM'     : _C.BOLD + _C.BLUE,
}

def log(level: str, msg: str, pid: int = None):
    pid = pid if pid is not None else os.getpid()
    ts  = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    c   = _COLORS.get(level, _C.RESET)
    print(
        f"{_C.BOLD}[{ts}]{_C.RESET} "
        f"[PID {pid:>6}] "
        f"{c}[{level:<8}]{_C.RESET} "
        f"{msg}",
        flush=True
    )

def info(msg):     log('INFO',     msg)
def success(msg):  log('SUCCESS',  msg)
def warning(msg):  log('WARNING',  msg)
def error(msg):    log('ERROR',    msg)
def worker(msg):   log('WORKER',   msg)
def conflict(msg): log('CONFLICT', msg)
def lock_log(msg): log('LOCK',     msg)
def sim(msg):      log('SIM',      msg)
def sep(title=''):
    line = '═' * 68
    if title:
        pad = (66 - len(title)) // 2
        print(f"\n{_C.BOLD}╔{line}╗{_C.RESET}")
        print(f"{_C.BOLD}║{' ' * pad}{title}{' ' * (66 - pad - len(title))}║{_C.RESET}")
        print(f"{_C.BOLD}╚{line}╝{_C.RESET}")
    else:
        print(f"{_C.BOLD}{'─' * 70}{_C.RESET}")
