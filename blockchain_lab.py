from datetime import datetime
import hashlib
import time


def calculate_hash(index, timestamp, data, previous_hash, creator="unknown", nonce=0):
    """
    Вычисление хэша блока на основе SHA-256.
    Поля разделяются символом '|', чтобы избежать коллизий склейки.
    """
    payload = "|".join([
        str(index),
        str(timestamp),
        str(data),
        str(previous_hash),
        str(creator),
        str(nonce),
    ]).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class Block:
    """Блок цепочки."""

    def __init__(self, index, timestamp, data, previous_hash,
                 creator="unknown", nonce=0):
        self.index = index
        self.timestamp = timestamp
        self.data = data
        self.previous_hash = previous_hash
        self.creator = creator
        self.nonce = nonce
        self.hash = calculate_hash(
            self.index, self.timestamp, self.data,
            self.previous_hash, self.creator, self.nonce
        )

    def mine_block(self, difficulty, should_stop=None):
        """
        Подбор nonce.
        should_stop — необязательный колбэк, возвращающий True для прерывания.
        """
        target = "0" * difficulty
        self.nonce = 0
        start = time.time()

        while True:
            if should_stop is not None and should_stop():
                raise InterruptedError("Майнинг прерван")
            self.hash = calculate_hash(
                self.index, self.timestamp, self.data,
                self.previous_hash, self.creator, self.nonce
            )
            if self.hash.startswith(target):
                break
            self.nonce += 1

        return time.time() - start

    def recalculate_hash(self):
        """Пересчитывает хэш блока (например, после изменения полей)."""
        self.hash = calculate_hash(
            self.index, self.timestamp, self.data,
            self.previous_hash, self.creator, self.nonce
        )
        return self.hash

    def to_dict(self):
        """Сериализация блока в словарь для передачи по сети (JSON)."""
        return {
            "index": self.index,
            "timestamp": self.timestamp,
            "data": self.data,
            "previous_hash": self.previous_hash,
            "creator": self.creator,
            "nonce": self.nonce,
            "hash": self.hash,
        }

    @staticmethod
    def from_dict(d):
        """Десериализация блока из словаря."""
        required = ("index", "timestamp", "data", "previous_hash", "hash")
        for key in required:
            if key not in d:
                raise ValueError(f"Отсутствует обязательное поле: {key}")

        block = Block(
            index=d["index"],
            timestamp=d["timestamp"],
            data=d["data"],
            previous_hash=d["previous_hash"],
            creator=d.get("creator", "unknown"),
            nonce=d.get("nonce", 0)
        )
        # Сохраняем оригинальный хэш (может отличаться от пересчитанного,
        # если данные были изменены — это позволяет обнаружить подделку)
        block.hash = d["hash"]
        return block

    def __repr__(self):
        return (
            f"Block(index={self.index}, creator='{self.creator}', "
            f"data='{self.data}', nonce={self.nonce}, "
            f"hash='{self.hash[:16]}...')"
        )


class Blockchain:
    """Цепочка блоков с консенсусом Proof-of-Work."""

    def __init__(self, difficulty=4):
        self.difficulty = difficulty
        self.chain = []
        self._create_genesis_block()

    def _create_genesis_block(self):
        """Создаёт генезис-блок (index=0, previous_hash='0')."""
        genesis = Block(
            index=0,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            data="Первый блок",
            previous_hash="0",
            creator="genesis",
            nonce=0
        )
        genesis.mine_block(self.difficulty)
        self.chain.append(genesis)

    def add_block(self, data, creator="человек"):
        """Добавляет новый блок в цепочку (для локального создания)."""
        if not self.chain:
            self._create_genesis_block()

        last = self.chain[-1]
        index = last.index + 1
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        previous_hash = last.hash

        new_block = Block(index, timestamp, data,
                          previous_hash, creator, nonce=0)
        elapsed = new_block.mine_block(self.difficulty)
        self.chain.append(new_block)
        return new_block, elapsed

    def add_block_without_validation(self, block):
        """
        Добавляет готовый блок от пира (майнинг уже выполнен).
        Проверяет только позицию index.
        """
        if not isinstance(block, Block):
            raise TypeError("Ожидается объект Block")

        if not self.chain:
            raise ValueError("Цепочка пуста — нет генезис-блока")

        expected_index = self.chain[-1].index + 1
        if block.index != expected_index:
            raise ValueError(
                f"Неверный index блока: получен {block.index}, "
                f"ожидался {expected_index}"
            )
        self.chain.append(block)

    def is_block_valid(self, block, chain=None):
        """Проверка отдельного блока."""
        if chain is None:
            chain = self.chain

        if not isinstance(block, Block):
            return False

        # 1. POW
        if not block.hash.startswith("0" * self.difficulty):
            return False

        # 2. Пересчёт хэша
        recalculated = calculate_hash(
            block.index, block.timestamp, block.data,
            block.previous_hash, block.creator, block.nonce
        )
        if recalculated != block.hash:
            return False

        # 3. Проверка генезис-блока
        if block.index == 0:
            return block.previous_hash == "0"

        # 4. Проверка связи с предыдущим блоком — ищем по индексу
        prev_block = next(
            (b for b in chain if b.index == block.index - 1), None
        )
        if prev_block is None:
            return False
        if block.previous_hash != prev_block.hash:
            return False

        return True

    def has_block_at_index(self, index):
        """Проверяет, есть ли уже блок с таким индексом."""
        return any(b.index == index for b in self.chain)

    def get_block_at_index(self, index):
        """Возвращает блок с указанным индексом или None."""
        return next((b for b in self.chain if b.index == index), None)

    def get_last_block(self):
        """Возвращает последний блок цепочки."""
        return self.chain[-1] if self.chain else None

    def replace_chain(self, new_chain):
        """
        Заменяет цепочку, если новая длиннее и валидна.
        При равной длине — побеждает цепочка с меньшим хэшем последнего
        блока (детерминированный tie-breaker для разрешения форков).
        Возвращает True, если замена произошла.
        """
        if not new_chain:
            return False

        # Сначала проверяем валидность новой цепочки
        saved = self.chain
        self.chain = list(new_chain)
        if not self.is_valid():
            self.chain = saved
            return False

        new_len = len(new_chain)
        cur_len = len(saved)

        if new_len > cur_len:
            return True

        if new_len < cur_len:
            self.chain = saved
            return False

        # Равная длина — сравниваем хэши последних блоков.
        # Меньший хэш = «более дорогой» при равном difficulty.
        new_tip = new_chain[-1].hash
        cur_tip = saved[-1].hash
        if new_tip < cur_tip:
            return True

        self.chain = saved
        return False

    def is_valid(self):
        """Полная проверка целостности цепочки."""
        if not self.chain:
            return False

        # 1. Проверка генезис-блока
        genesis = self.chain[0]
        if genesis.index != 0 or genesis.previous_hash != "0":
            return False

        # 2. Проверка каждого блока
        for i, block in enumerate(self.chain):
            # Порядок индексов (должны идти 0, 1, 2, ...)
            if block.index != i:
                return False

            # Связь с предыдущим блоком
            if i > 0:
                if block.previous_hash != self.chain[i - 1].hash:
                    return False

            # PoW: hash должен начинаться с difficulty нулей
            if not block.hash.startswith("0" * self.difficulty):
                return False

            # Пересчёт хэша
            recalculated = calculate_hash(
                block.index, block.timestamp, block.data,
                block.previous_hash, block.creator, block.nonce
            )
            if recalculated != block.hash:
                return False

        return True

    def __len__(self):
        return len(self.chain)

    def __repr__(self):
        return (f"Blockchain(length={len(self.chain)}, "
                f"difficulty={self.difficulty})")


# Демонстрация работы модуля
if __name__ == "__main__":
    DIFFICULTY = 4
    print(f"=== Демонстрация PoW (difficulty={DIFFICULTY}) ===")

    bc = Blockchain(difficulty=DIFFICULTY)
    for i, doc in enumerate([
        "Документ A передан от Иванова к Петрову",
        "Документ B передан от Петрова к Сидорову",
        "Документ C передан от Сидорова к Кузнецову",
    ], start=1):
        block, elapsed = bc.add_block(doc, creator="demo")
        print(f"  Блок #{block.index} намайнен за {elapsed:.2f} с, "
              f"nonce={block.nonce}, hash={block.hash[:16]}...")

    print("\n=== Цепочка ===")
    for block in bc.chain:
        print(" ", block)

    print("\n=== Проверка целостности ===")
    print("Цепочка валидна:", bc.is_valid())

    # Подделка
    print("\n=== Подделка блока index=2 без майнинга ===")
    victim = bc.chain[2]
    victim.data = "ПОДДЕЛКА: Документ B передан неизвестному лицу"
    victim.recalculate_hash()
    print(f"  Новый хеш: {victim.hash[:16]}... "
          f"(не начинается с {'0' * DIFFICULTY})")
    print("  Цепочка валидна:", bc.is_valid(), " ← должно быть False")

    print("\n=== Попытка перемайнить подделку ===")
    t0 = time.time()
    victim.mine_block(DIFFICULTY)
    print(f"  Перемайнено за {time.time() - t0:.2f} с, "
          f"nonce={victim.nonce}")
    print("  Цепочка валидна:", bc.is_valid(),
          " ← False, т.к. следующий блок ссылается на старый hash")