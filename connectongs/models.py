from dataclasses import dataclass
from typing import Optional


@dataclass
class Usuario:
    id: int
    nome: str
    email: str
    senha_hash: str
    tipo: str        # "restaurante" | "ong" | "admin"
    regiao: str = "Plano Piloto"
    ativo: bool = True


@dataclass
class Alimento:
    id: int
    nome: str
    quantidade: int
    descricao: str
    data_validade: str   # YYYY-MM-DD
    restaurante_id: int
    restaurante_nome: str
    regiao: str
    status: str = "disponivel"  # disponivel | reservado | concluido | vencido


@dataclass
class Reserva:
    id: int
    alimento_id: int
    alimento_nome: str
    ong_id: int
    ong_nome: str
    data_reserva: str
    status: str = "pendente"    # pendente | concluido | cancelado


@dataclass
class Notificacao:
    usuario_destino_id: int
    mensagem: str
    tipo: str            # "nova_doacao" | "reserva_confirmada" | "reserva_cancelada"
    alimento_id: Optional[int] = None
    lida: bool = False
