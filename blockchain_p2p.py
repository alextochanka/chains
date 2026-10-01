import json
import socket
import threading
import time
from datetime import datetime

import matplotlib

try:
    matplotlib.use("TkAgg")
except Exception:
    matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

from blockchain_lab import Block, Blockchain


BUFFER_SIZE = 65536
ENCODING = "utf-8"
MSG_DELIMITER = b"\n"
SOCKET_TIMEOUT = 30.0
CONNECT_TIMEOUT = 3.0

# Визуализация
def visualize_chain(nodes, title="Состояние цепочек блоков", save_path=None):
    fig, ax = plt.subplots(figsize=(max(8, 3 * len(nodes)), 6))

    creators = sorted({b.creator for n in nodes for b in n.blockchain.chain})
    cmap = plt.get_cmap("tab10")
    color_map = {c: cmap(i % 10) for i, c in enumerate(creators)}

    box_w, box_h = 1.6, 0.9
    y_step, x_step = 1.2, 1.9

    for row, node in enumerate(nodes):
        y = -row * y_step
        ax.text(-1.2, y + box_h / 2, node.node_id,
                ha="right", va="center", fontsize=12, fontweight="bold")

        for block in node.blockchain.chain:
            x = block.index * x_step
            color = color_map[block.creator]

            rect = FancyBboxPatch(
                (x, y), box_w, box_h,
                boxstyle="round,pad=0.05",
                linewidth=1.5, edgecolor="black",
                facecolor=color, alpha=0.75,
            )
            ax.add_patch(rect)

            ax.text(x + box_w / 2, y + box_h * 0.7,
                    f"#{block.index}",
                    ha="center", va="center",
                    fontsize=10, fontweight="bold")
            ax.text(x + box_w / 2, y + box_h * 0.4,
                    f"{block.creator}",
                    ha="center", va="center", fontsize=8)
            ax.text(x + box_w / 2, y + box_h * 0.15,
                    f"n={block.nonce}",
                    ha="center", va="center", fontsize=7, style="italic")

            if block.index > 0:
                ax.annotate(
                    "",
                    xy=(x, y + box_h / 2),
                    xytext=(x - x_step + box_w, y + box_h / 2),
                    arrowprops=dict(arrowstyle="->", color="gray", lw=1.2),
                )

    handles = [
        plt.Line2D([0], [0], marker="s", color="w",
                   markerfacecolor=color_map[c], markersize=12, label=c)
        for c in creators
    ]
    ax.legend(handles=handles, loc="upper right", title="Создатель блока")

    max_index = max((b.index for n in nodes for b in n.blockchain.chain),
                    default=0)
    ax.set_xlim(-2.5, (max_index + 1) * x_step + 1)
    ax.set_ylim(-(len(nodes) - 1) * y_step - 1, 1.5)
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.axis("off")
    plt.tight_layout()

    if save_path:
        try:
            plt.savefig(save_path, dpi=150, bbox_inches="tight")
            print(f"📊 График сохранён: {save_path}")
        except Exception as e:
            print(f"⚠️  Не удалось сохранить график: {e}")

    if matplotlib.get_backend().lower() != "agg":
        plt.show()
    else:
        plt.close(fig)

# Узел сети
class Node:
    def __init__(self, node_id, host="127.0.0.1", port=5000, difficulty=5):
        self.node_id = node_id
        self.host = host
        self.port = port

        self.blockchain = Blockchain(difficulty=difficulty)
        self.peers = {}
        self.peers_lock = threading.Lock()
        self.chain_lock = threading.Lock()
        # Защита от одновременного майнинга двух блоков в одном узле
        self.mining_lock = threading.Lock()

        self.server_socket = None
        self.running = False

        self.log = []
        self.log_lock = threading.Lock()

        # Кэш известных хэшей блоков — для подавления дубликатов
        self.known_hashes = set()
        self.known_hashes_lock = threading.Lock()

    # Логирование
    def _log(self, message):
        entry = f"[{datetime.now().strftime('%H:%M:%S')}] [{self.node_id}] {message}"
        with self.log_lock:
            self.log.append(entry)
            print(entry)  # print внутри лока — иначе строки перемешиваются

    # Кэш хэшей
    def _is_known_hash(self, block_hash):
        """True, если такой хэш уже видели. Иначе запоминает его."""
        with self.known_hashes_lock:
            if block_hash in self.known_hashes:
                return True
            self.known_hashes.add(block_hash)
            return False

    # Соседи
    def add_peer(self, peer, host=None, port=None):
        if isinstance(peer, Node):
            peer_id, host, port = peer.node_id, peer.host, peer.port
        else:
            peer_id = peer
            if host is None:
                host = "127.0.0.1"
            if port is None:
                raise ValueError("Для строкового peer_id необходимо указать порт")

        with self.peers_lock:
            self.peers[peer_id] = (str(host), int(port))

        self._log(f"Добавлен peer: {peer_id} -> {host}:{port}")

        # Запрашиваем цепочку у нового пира, указывая СВОЙ серверный адрес
        threading.Thread(
            target=self._send_to_peer,
            args=(str(host), int(port), {
                "type": "get_chain",
                "node_id": self.node_id,
                "host": self.host,
                "port": self.port,
            }),
            daemon=True,
        ).start()

    # TCP-сервер
    def start_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(16)
        self.running = True
        self._log(f"TCP-сервер запущен на {self.host}:{self.port}")
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _accept_loop(self):
        while self.running:
            try:
                client_sock, addr = self.server_socket.accept()
            except (OSError, ValueError):
                break
            if not self.running:
                client_sock.close()
                break
            threading.Thread(
                target=self._handle_client, args=(client_sock, addr),
                daemon=True
            ).start()

    def _handle_client(self, client_sock, addr):
        client_sock.settimeout(SOCKET_TIMEOUT)
        try:
            buffer = b""
            while self.running:
                try:
                    chunk = client_sock.recv(BUFFER_SIZE)
                except socket.timeout:
                    self._log(f"Таймаут соединения с {addr}")
                    break
                if not chunk:
                    break
                buffer += chunk
                while MSG_DELIMITER in buffer:
                    raw, buffer = buffer.split(MSG_DELIMITER, 1)
                    if not raw.strip():
                        continue
                    try:
                        message = json.loads(raw.decode(ENCODING))
                    except json.JSONDecodeError as e:
                        self._log(f"Ошибка разбора JSON от {addr}: {e}")
                        continue
                    self._process_message(message, addr)
        except OSError as e:
            self._log(f"Соединение с {addr} прервано: {e}")
        finally:
            client_sock.close()

    # Обработка сообщений
    def _process_message(self, message, addr):
        msg_type = message.get("type")

        if msg_type == "block":
            try:
                block = Block.from_dict(message["block"])
            except (KeyError, ValueError) as e:
                self._log(f"Некорректный блок от {addr}: {e}")
                return
            if block.creator == self.node_id:
                return
            # Подавляем дубликаты
            if self._is_known_hash(block.hash):
                return
            self._log(f"📥 Получен блок #{block.index} от {block.creator} "
                      f"(nonce={block.nonce}, hash={block.hash[:10]}...)")
            self.receive_block(block)

        elif msg_type == "chain":
            try:
                incoming = [Block.from_dict(d) for d in message["chain"]]
            except (KeyError, ValueError) as e:
                self._log(f"Некорректная цепочка от {addr}: {e}")
                return
            # Если входящая цепочка идентична нашей — молча игнорируем
            with self.chain_lock:
                my_len = len(self.blockchain.chain)
                my_tip = self.blockchain.chain[-1].hash if self.blockchain.chain else None
            in_len = len(incoming)
            in_tip = incoming[-1].hash if incoming else None
            if my_len == in_len and my_tip == in_tip:
                return  # идентичная цепочка — не шумим
            with self.chain_lock:
                replaced = self.blockchain.replace_chain(incoming)
                new_len = len(self.blockchain.chain)
                new_tip = self.blockchain.chain[-1].hash[:10]
            if replaced:
                self._log(f"🔁 Цепочка заменена (len={new_len}, tip={new_tip}...)")
                self.broadcast_chain()
            else:
                self._log(f"Цепочка от {addr} не принята (не сильнее/невалидна)")

        elif msg_type == "get_chain":
            # Отвечаем на СЕРВЕРНЫЙ адрес отправителя, а не на addr клиента
            sender_id = message.get("node_id")
            sender_host = message.get("host")
            sender_port = message.get("port")

            if not (sender_host and sender_port):
                with self.peers_lock:
                    if sender_id in self.peers:
                        sender_host, sender_port = self.peers[sender_id]

            if not (sender_host and sender_port):
                self._log(f"⚠️  get_chain от {addr}: не удалось определить "
                          f"серверный адрес отправителя")
                return

            with self.chain_lock:
                payload = {
                    "type": "chain",
                    "chain": [b.to_dict() for b in self.blockchain.chain],
                }
            self._send_to_peer(sender_host, sender_port, payload)

        elif msg_type == "hello":
            self._log(f"Hello от {message.get('node_id')} @ {addr}")

        else:
            self._log(f"Неизвестный тип сообщения: {msg_type}")

    # Отправка
    def _send_to_peer(self, host, port, payload):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(CONNECT_TIMEOUT)
                s.connect((str(host), int(port)))
                data = json.dumps(payload).encode(ENCODING) + MSG_DELIMITER
                s.sendall(data)
            return True
        except (OSError, socket.timeout, ValueError) as e:
            self._log(f"Не удалось отправить в {host}:{port}: {e}")
            return False

    def broadcast_block(self, block, exclude_peer_id=None):
        with self.peers_lock:
            peers_snapshot = list(self.peers.items())
        payload = {"type": "block", "block": block.to_dict()}
        for peer_id, (host, port) in peers_snapshot:
            if exclude_peer_id and peer_id == exclude_peer_id:
                continue
            threading.Thread(
                target=self._send_to_peer, args=(host, port, payload),
                daemon=True
            ).start()

    def broadcast_chain(self):
        with self.peers_lock:
            peers_snapshot = list(self.peers.items())
        with self.chain_lock:
            payload = {
                "type": "chain",
                "chain": [b.to_dict() for b in self.blockchain.chain],
            }
        for peer_id, (host, port) in peers_snapshot:
            threading.Thread(
                target=self._send_to_peer, args=(host, port, payload),
                daemon=True
            ).start()

    def _request_chain_from_peers(self):
        """Рассылает get_chain всем пирам — используется при обнаружении форка."""
        with self.peers_lock:
            peers_snapshot = list(self.peers.items())
        payload = {
            "type": "get_chain",
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
        }
        for peer_id, (host, port) in peers_snapshot:
            threading.Thread(
                target=self._send_to_peer, args=(host, port, payload),
                daemon=True,
            ).start()

    # Приём блока (с проверкой PoW и обработкой форков)
    def receive_block(self, block):
        request_chain = False

        with self.chain_lock:
            last = self.blockchain.chain[-1] if self.blockchain.chain else None
            if last is None:
                self._log(f"❌ Отклонён блок #{block.index}: нет генезис-блока")
                return False

            expected_index = last.index + 1

            # Уже есть блок с таким индексом
            if block.index < expected_index:
                existing = self.blockchain.get_block_at_index(block.index)
                if existing is not None and existing.hash != block.hash:
                    # Это форк — просим у пиров их цепочку
                    self._log(f"🔀 Обнаружен форк на #{block.index}: "
                              f"свой={existing.hash[:10]}..., "
                              f"чужой={block.hash[:10]}... "
                              f"→ запрашиваю цепочку")
                    request_chain = True
                else:
                    self._log(f"⏭️  Пропущен устаревший блок #{block.index} "
                              f"от {block.creator}")
                # вне блока if — если request_chain, всё равно выходим из лока
            elif block.index > expected_index:
                self._log(f"❌ Отклонён блок #{block.index} от {block.creator}: "
                          f"разрыв (ожидался #{expected_index})")
                return False

            else:
                # Обычный случай: индекс совпадает с ожидаемым
                if not self.blockchain.is_block_valid(
                        block, chain=self.blockchain.chain):
                    self._log(f"❌ Отклонён блок #{block.index} "
                              f"от {block.creator}: невалидный PoW/хеш")
                    return False

                if block.previous_hash != last.hash:
                    self._log(f"❌ Отклонён блок #{block.index} "
                              f"от {block.creator}: previous_hash не совпадает")
                    return False

                try:
                    self.blockchain.add_block_without_validation(block)
                    self._log(f"✅ Принят блок #{block.index} "
                              f"от {block.creator} "
                              f"(nonce={block.nonce}, hash={block.hash[:10]}...)")
                    accepted = True
                except ValueError as e:
                    self._log(f"❌ Отклонён блок #{block.index}: {e}")
                    accepted = False

        # Вне chain_lock — сетевые операции
        if request_chain:
            self._request_chain_from_peers()
            return False

        if accepted:
            self.broadcast_block(block)
            return True
        return False

    # Создание (майнинг) блока
    def create_block(self, data):
        with self.mining_lock:
            with self.chain_lock:
                last = self.blockchain.chain[-1]
                expected_index = last.index + 1
                expected_prev_hash = last.hash
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            candidate = Block(
                index=expected_index,
                timestamp=timestamp,
                data=data,
                previous_hash=expected_prev_hash,
                creator=self.node_id,
                nonce=0,
            )
            self._log(f"⛏️  Начат майнинг блока #{expected_index} "
                      f"(difficulty={self.blockchain.difficulty})...")
            try:
                elapsed = candidate.mine_block(
                    self.blockchain.difficulty,
                    should_stop=lambda: not self.running,
                )
            except InterruptedError:
                self._log(f"🛑 Майнинг блока #{expected_index} прерван")
                return None

            self._log(f"🔨 Блок #{candidate.index} намайнен за {elapsed:.2f} с "
                      f"(nonce={candidate.nonce}, hash={candidate.hash[:10]}...)")

        with self.chain_lock:
            last = self.blockchain.chain[-1]
            if last.index + 1 != candidate.index:
                self._log(f"⚠️  Свой блок #{candidate.index} отброшен: "
                          f"цепочка уже продвинулась (last=#{last.index})")
                return None
            if last.hash != candidate.previous_hash:
                self._log(f"⚠️  Свой блок #{candidate.index} отброшен: "
                          f"previous_hash не совпадает")
                return None

            self.blockchain.chain.append(candidate)
            self._log(f"✅ Свой блок #{candidate.index} добавлен в цепочку")

        # Запоминаем свой хэш, чтобы не реагировать на его ретрансляцию
        self._is_known_hash(candidate.hash)
        self.broadcast_block(candidate)
        return candidate

    # Остановка
    def stop(self):
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.server_socket.close()
            except OSError:
                pass
        time.sleep(0.1)
        self._log("Остановлен")

    def __repr__(self):
        return (f"Node(id='{self.node_id}', {self.host}:{self.port}, "
                f"chain_len={len(self.blockchain.chain)}, "
                f"difficulty={self.blockchain.difficulty})")


# Демонстрация
def main():
    print("=" * 70)
    print("P2P-БЛОКЧЕЙН С PROOF-OF-WORK (ЛР №4)")
    print("=" * 70)

    DIFFICULTY = 4  # подберите 3-4, чтобы майнинг занимал 1-5 секунд

    node1 = Node("Node_1", port=5001, difficulty=DIFFICULTY)
    node2 = Node("Node_2", port=5002, difficulty=DIFFICULTY)
    node3 = Node("Node_3", port=5003, difficulty=DIFFICULTY)

    node1.start_server()
    node2.start_server()
    node3.start_server()
    time.sleep(0.5)

    # Связываем узлы (add_peer сам запросит цепочку у нового пира)
    node1.add_peer("Node_2", "127.0.0.1", 5002)
    node1.add_peer("Node_3", "127.0.0.1", 5003)
    node2.add_peer("Node_1", "127.0.0.1", 5001)
    node2.add_peer("Node_3", "127.0.0.1", 5003)
    node3.add_peer("Node_1", "127.0.0.1", 5001)
    node3.add_peer("Node_2", "127.0.0.1", 5002)

    time.sleep(1.5)  # даём время на обмен цепочками

    print("\n--- Начальное состояние ---")
    for n in (node1, node2, node3):
        print(f"  {n.node_id}: длина = {len(n.blockchain.chain)}, "
              f"последний = {n.blockchain.chain[-1].data!r}, "
              f"difficulty = {n.blockchain.difficulty}")

    # Сценарий 1: один узел майнит
    print("\n" + "=" * 70)
    print("СЦЕНАРИЙ 1: Node_1 майнит блок 'Транзакция А'")
    print("=" * 70)
    node1.create_block("Транзакция А")
    time.sleep(2.0)

    for n in (node1, node2, node3):
        last = n.blockchain.chain[-1]
        print(f"  {n.node_id}: index={last.index}, "
              f"data={last.data!r}, nonce={last.nonce}, "
              f"len={len(n.blockchain.chain)}, "
              f"valid={n.blockchain.is_valid()}")

    visualize_chain(
        [node1, node2, node3],
        title=f"После сценария 1 (difficulty={DIFFICULTY})",
        save_path="chain_after_scenario_1_pow.png",
    )

    # Сценарий 2: гонка майнеров
    # Сценарий 2: гарантированный форк
    print("\n" + "=" * 70)
    print("СЦЕНАРИЙ 2: ГАРАНТИРОВАННЫЙ ФОРК")
    print("=" * 70)

    # Временно отключаем рассылку у обоих узлов
    orig_broadcast_1 = node1.broadcast_block
    orig_broadcast_2 = node2.broadcast_block
    node1.broadcast_block = lambda *a, **kw: None
    node2.broadcast_block = lambda *a, **kw: None

    t1 = threading.Thread(target=node1.create_block, args=("Транзакция X",))
    t2 = threading.Thread(target=node2.create_block, args=("Транзакция Y",))
    t1.start();
    t2.start()
    t1.join();
    t2.join()

    # Возвращаем рассылку
    node1.broadcast_block = orig_broadcast_1
    node2.broadcast_block = orig_broadcast_2

    # Теперь каждый узел имеет свою ветку. Запускаем разрешение форка:
    print("\n--- Разрешение форка ---")
    node1.broadcast_block(node1.blockchain.chain[-1])
    node2.broadcast_block(node2.blockchain.chain[-1])

    time.sleep(4.0)

    visualize_chain(
        [node1, node2, node3],
        title="После разрешения форка",
        save_path="chain_after_fork_resolution.png",
    )

    # Итог
    print("\n" + "=" * 70)
    print("ИТОГОВОЕ СОСТОЯНИЕ ЦЕПОЧЕК")
    print("=" * 70)
    for n in (node1, node2, node3):
        last = n.blockchain.chain[-1]
        print(f"  {n.node_id}: last index={last.index}, "
              f"data={last.data!r}, creator={last.creator!r}, "
              f"nonce={last.nonce}, len={len(n.blockchain.chain)}, "
              f"is_valid={n.blockchain.is_valid()}")

    h1 = [b.hash for b in node1.blockchain.chain]
    h2 = [b.hash for b in node2.blockchain.chain]
    h3 = [b.hash for b in node3.blockchain.chain]

    if h1 == h2 == h3:
        print("\n✅ КОНСЕНСУС ДОСТИГНУТ: все узлы имеют одинаковую цепочку.")
    else:
        print("\n⚠️  Цепочки разошлись (даже после разрешения форка).")
        for n, ch in ((node1, h1), (node2, h2), (node3, h3)):
            print(f"   {n.node_id}: {ch}")

    # Шаг 6: защита от подделки
    print("\n" + "=" * 70)
    print("ШАГ 6: ПРОВЕРКА ЗАЩИТЫ ОТ ПОДДЕЛКИ")
    print("=" * 70)

    victim_node = node1
    if len(victim_node.blockchain.chain) > 1:
        target = victim_node.blockchain.chain[1]
        print(f"  Исходный блок #{target.index}: data={target.data!r}, "
              f"nonce={target.nonce}, hash={target.hash[:16]}...")

        # Портим данные, nonce оставляем старый, хеш пересчитываем
        target.data = "ПОДДЕЛКА!"
        target.recalculate_hash()
        print(f"  После подделки: data={target.data!r}, "
              f"hash={target.hash[:16]}...")
        print(f"  is_valid() = {victim_node.blockchain.is_valid()}  "
              f"← False: хеш не начинается с {'0' * DIFFICULTY}")

        # Пытаемся перемайнить подделку
        t0 = time.time()
        target.mine_block(DIFFICULTY)
        elapsed = time.time() - t0
        print(f"  Перемайнено за {elapsed:.2f} с, nonce={target.nonce}, "
              f"hash={target.hash[:16]}...")
        print(f"  is_valid() = {victim_node.blockchain.is_valid()}  "
              f"← False: следующий блок ссылается на старый hash")

    # Логи
    print("\n" + "=" * 70)
    print("ЖУРНАЛЫ СОБЫТИЙ")
    print("=" * 70)
    for n in (node1, node2, node3):
        print(f"\n===== {n.node_id} =====")
        for line in n.log:
            print("  " + line)

    print("\n--- Остановка узлов ---")
    for n in (node1, node2, node3):
        n.stop()


if __name__ == "__main__":
    main()