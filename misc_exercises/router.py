import asyncio
import time
import argparse
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

# Porta fixa (UDP) para os roteadores
PORT = 6000

# Intervalos (ajuste conforme preferir)
ANNOUNCE_INTERVAL = 10  # segundos entre anúncios de rotas e snapshots
FAILURE_TIMEOUT   = 15  # segundos para considerar vizinho inativo

@dataclass
class Route:
    """Entrada da tabela de roteamento."""
    dest: str
    metric: int
    next_hop: str

class RouterProtocol(asyncio.DatagramProtocol):
    """
    Roteamento por vetor de distância (sem split horizon), com:
      - anúncio periódico de rotas (*dest;metric*...)
      - anúncio de roteador (@<ip>)
      - mensagens fim-a-fim (!orig;dest;texto)
      - detecção de falha e withdraw
    """
    def __init__(self, my_ip: str, neighbors: List[str]):
        self.my_ip = my_ip
        self.neighbors = set(neighbors)
        # tabela de rotas: dest -> Route
        self.table: Dict[str, Route] = {n: Route(dest=n, metric=1, next_hop=n) for n in neighbors}
        # último contato de cada vizinho
        self.last_seen: Dict[str, float] = {n: time.time() for n in neighbors}
        # destinos anunciados por cada vizinho no último ciclo (para withdraw)
        self.advertised_by_neighbor: Dict[str, set] = {n: set() for n in neighbors}
        self.transport: Optional[asyncio.DatagramTransport] = None

    # ===== Lifecycle =====
    def connection_made(self, transport):
        self.transport = transport
        # anuncia o próprio roteador aos vizinhos
        for n in self.neighbors:
            self._send_to(n, f'@{self.my_ip}')
        # tarefas periódicas
        asyncio.create_task(self._announce_loop())
        asyncio.create_task(self._failure_detection_loop())
        asyncio.create_task(self._print_loop())
        # melhora: anuncia imediatamente (não espera o próximo ciclo)
        self.announce()

    def datagram_received(self, data: bytes, addr):
        message = data.decode('utf-8', errors='ignore').strip()
        origin = addr[0]
        self.last_seen[origin] = time.time()
        if not message:
            return

        if message.startswith('*'):
            self.handle_route_announcement(message, origin)
        elif message.startswith('@'):
            ip = message[1:]
            if ip and ip != self.my_ip:
                if ip not in self.neighbors:
                    self.neighbors.add(ip)
                    self.last_seen[ip] = time.time()
                    self.advertised_by_neighbor[ip] = set()
                if (ip not in self.table) or (self.table[ip].metric > 1) or (self.table[ip].next_hop != ip):
                    self.table[ip] = Route(dest=ip, metric=1, next_hop=ip)
                    self.print_table(f'Roteador {ip} anunciado via {origin}')
                    self.announce()
        elif message.startswith('!'):
            self.handle_text_message(message)
        # demais: ignora

    # ===== Anúncios de rotas =====
    def handle_route_announcement(self, msg: str, origin: str):
        """Formato: *dest;metric*dest;metric..."""
        parts = [p for p in msg.split('*') if p]
        updated = False
        new_set = set()  # destinos anunciados por 'origin' neste ciclo

        for part in parts:
            try:
                ip, m = part.split(';')
                metric = int(m)
            except ValueError:
                continue
            if ip == self.my_ip:
                continue  # ignora rota para si mesmo

            new_metric = metric + 1
            new_set.add(ip)
            if ip not in self.table or new_metric < self.table[ip].metric:
                self.table[ip] = Route(dest=ip, metric=new_metric, next_hop=origin)
                updated = True

        # withdraw: destinos que origin anunciava antes e agora não
        prev_set = self.advertised_by_neighbor.get(origin, set())
        withdrawn = prev_set - new_set
        for w_ip in list(withdrawn):
            if w_ip in self.table and self.table[w_ip].next_hop == origin:
                self.table.pop(w_ip)
                updated = True

        self.advertised_by_neighbor[origin] = new_set

        if updated:
            self.print_table(f'Rotas atualizadas a partir de {origin}')
            self.announce()

    def announce(self):
        """
        Envia a TABELA COMPLETA para todos os vizinhos (SEM split horizon).
        Formato: *dest;metric*dest;metric...
        """
        if not self.transport:
            return
        payload = ''.join(f'*{dest};{route.metric}' for dest, route in self.table.items())
        if not payload:
            return
        for neighbor in list(self.neighbors):
            self._send_to(neighbor, payload)

    # ===== Mensagens de texto =====
    def handle_text_message(self, msg: str):
        """Formato: !orig;dest;texto"""
        try:
            _, rest = msg[0], msg[1:]
            origin_ip, dest_ip, text = rest.split(';', 2)
        except ValueError:
            return

        if dest_ip == self.my_ip:
            print(f'Recebido de {origin_ip} -> {dest_ip}: "{text}" (entregue)')
            return

        route = self.table.get(dest_ip)
        if route:
            forward = f'!{origin_ip};{dest_ip};{text}'
            self._send_to(route.next_hop, forward)
            print(f'Encaminhando {origin_ip}->{dest_ip} via {route.next_hop}')
        else:
            print(f'Sem rota para {dest_ip}; descartando mensagem de {origin_ip}')

    # ===== Loops periódicos =====
    def _send_to(self, ip: str, message: str):
        if self.transport:
            try:
                self.transport.sendto(message.encode('utf-8'), (ip, PORT))
            except Exception as e:
                print(f'Erro ao enviar para {ip}: {e}')

    async def _announce_loop(self):
        while True:
            self.announce()
            await asyncio.sleep(ANNOUNCE_INTERVAL)

    async def _print_loop(self):
        while True:
            self.print_table('Snapshot periódico')
            await asyncio.sleep(ANNOUNCE_INTERVAL)

    async def _failure_detection_loop(self):
        while True:
            now = time.time()
            inactive = [n for n, t in list(self.last_seen.items()) if now - t > FAILURE_TIMEOUT]
            for n in inactive:
                print(f'⚠️  Vizinho inativo: {n} — removendo rotas')
                self.neighbors.discard(n)
                self.last_seen.pop(n, None)
                self.advertised_by_neighbor.pop(n, None)
                removed = False
                for dest in list(self.table.keys()):
                    if dest == n or self.table[dest].next_hop == n:
                        self.table.pop(dest)
                        removed = True
                if removed:
                    self.print_table(f'Rotas via {n} removidas (inativo)')
                    self.announce()
            await asyncio.sleep(1)

    # ===== Utilitários =====
    def print_table(self, reason: str):
        print(f'\n--- {reason} @ {self.my_ip} ---')
        print(f'{"Destination":<15} {"Metric":<6} {"Next Hop":<15}')
        for dest, route in sorted(self.table.items()):
            print(f'{dest:<15} {route.metric:<6} {route.next_hop:<15}')
        print('-----------------------------')

# ===== Interface interativa =====
async def interactive_input(protocol: RouterProtocol):
    """
    Comandos:
      - msg <dest_ip> <texto>
      - show
    """
    loop = asyncio.get_running_loop()
    while True:
        try:
            user_input = await loop.run_in_executor(None, input, 'Comando (msg <dest> <texto> | show): ')
        except (EOFError, KeyboardInterrupt):
            break
        parts = user_input.strip().split(' ', 2)
        if not parts:
            continue
        cmd = parts[0].lower()
        if cmd == 'msg' and len(parts) >= 3:
            dest_ip, text = parts[1], parts[2]
            route = protocol.table.get(dest_ip)
            if route:
                message = f'!{protocol.my_ip};{dest_ip};{text}'
                protocol._send_to(route.next_hop, message)
                print(f'Enviado para {dest_ip} via {route.next_hop}')
            else:
                print(f'Sem rota para {dest_ip}')
        elif cmd == 'show':
            protocol.print_table('Inspeção manual')
        else:
            print('Comando inválido.')

# ===== Main =====
async def main():
    parser = argparse.ArgumentParser(description='Roteador (vetor de distância) — sem split horizon')
    parser.add_argument('my_ip', help='IP deste roteador (string).')
    parser.add_argument('--config', default='roteadores.txt', help='Arquivo com vizinhos (um IP por linha).')
    args = parser.parse_args()

    neighbors = []
    if args.config and os.path.isfile(args.config):
        with open(args.config, 'r') as f:
            for line in f:
                ip = line.strip()
                if ip and ip != args.my_ip:
                    neighbors.append(ip)

    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: RouterProtocol(args.my_ip, neighbors),
        local_addr=(args.my_ip, PORT)
    )

    await interactive_input(protocol)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Encerrado pelo usuário.')
