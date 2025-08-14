from asyncio import Lock, CancelledError, sleep
from collections import defaultdict
from logging import getLogger
from time import monotonic

logger = getLogger('my_app')


class RateLimiter:
    """
    Класс для контроля частоты запросов.
    Между последовательными запросами проходит не менее `interval` секунд.
    """

    def __init__(self, interval: float = 1.0):
        self.interval = interval  # Минимальный интервал между запросами (в секундах)
        self._last_request_time: float = 0.0  # Время последнего разрешенного запроса
        self._lock = Lock()  # Блокировка для защиты _last_request_time от гонок

    async def wait_for_permission(self, symbol: str):
        """
        Метод блокирует выполнение, если необходимо выждать интервал.
        """
        async with self._lock:  # Захватываем блокировку, чтобы только одна задача могла проверять/обновлять
            now = monotonic()  # Используем time.monotonic() для надежного отсчета времени
            elapsed_time = now - self._last_request_time

            if elapsed_time < self.interval:
                time_to_wait = self.interval - elapsed_time
                logger.warning(f"\nСлишком рано {symbol}. Ожидание {time_to_wait:.2f} сек...")
                await sleep(time_to_wait)

            self._last_request_time = monotonic()  # Обновляем время последнего запроса после ожидания (если оно было)


class ConfigManager:
    def __init__(self):
        self.symbols = []
        self._data = defaultdict(dict)
        self._lock = Lock()

    async def load_config(self, batch_data: dict):
        async with self._lock:
            for data in batch_data:
                self.symbols.append(data.symbol_name)
                self._data[data.symbol_name].update({
                    'init_grid_step': data.init_grid_step,
                    'grid_size': data.grid_size,
                    'price_step': data.price_step,
                    'lot_b': data.lot_b,
                    'lot_s': data.lot_s,
                })

    async def set_data(self, symbol: str, key: str, value: float | bool):
        async with self._lock:
            self._data[symbol][key] = value

    async def get_data(self, symbol: str, key: str):
        async with self._lock:
            return self._data.get(symbol).get(key)


class TaskManager:  # Класс для работы с задачами
    def __init__(self):
        self._tasks = defaultdict(list)
        self._lock = Lock()

    async def add_task(self, symbol: str, task):
        async with self._lock:
            self._tasks[symbol].append(task)

    async def del_tasks(self, symbol: str):
        async with self._lock:
            for task in self._tasks.pop(symbol, []):
                if task and not task.done():  # Проверяем, что задача существует и не завершена
                    task.cancel()
                    try:
                        await task  # Дожидаемся завершения задачи
                    except CancelledError:
                        pass  # Игнорируем CancelledError - это ожидаемое поведение


class WebSocketPrice:  # Класс для работы с ценами в реальном времени из websockets
    def __init__(self):
        self._data = {}
        self._lock = Lock()

    async def update_price(self, symbol: str, time: int, price: float):
        async with self._lock:
            self._data[symbol] = time, price

    async def get_price(self, symbol: str):
        async with self._lock:
            return self._data.get(symbol)


class AccountManager:  # Класс для работы с данными счета
    def __init__(self):
        self._listen_key = None
        self._lock = Lock()

    async def add_listen_key(self, listen_key: str):
        async with self._lock:
            self._listen_key = listen_key

    async def get_listen_key(self):
        async with self._lock:
            return self._listen_key


class SymbolOrderManager:  # Класс для работы с ордерами в реальном времени
    def __init__(self):
        self.symbols = []
        self._data = defaultdict(self._create_default_symbol_data)
        self._lock = Lock()

    @staticmethod
    def _create_default_symbol_data():
        return {'state': 'stop',
                'profit': 0.0,
                'total_lot_b': 0,
                'total_lot_s': 0,
                'risk_rate': (0, False),
                # 'b_s_trigger': 'new',
                'orders': []}

    async def add_symbols_and_orders(self, batch_data: list):
        async with self._lock:
            for symbol, orders in batch_data:
                self.symbols.append(symbol.name)
                self._data[symbol.name].update({
                    'state': symbol.state,
                    'profit': symbol.profit,
                    'orders': orders
                })

    # async def set_b_s_trigger(self, symbol: str, trigger: str):
    #     async with self._lock:
    #         self._data[symbol]['b_s_trigger'] = trigger
    #
    # async def get_b_s_trigger(self, symbol: str):
    #     async with self._lock:
    #         return self._data.get(symbol).get('b_s_trigger')

    async def set_grid_boundaries(self, symbol: str, grid_boundaries: list):
        async with self._lock:
            self._data[symbol]['grid_boundaries'] = grid_boundaries

    async def get_grid_boundaries(self, symbol: str):
        async with self._lock:
            return self._data.get(symbol).get('grid_boundaries')

    async def set_risk_rate(self, symbol: str, value):
        async with self._lock:
            self._data[symbol]['risk_rate'] = (value, True)

    async def get_risk_rate(self, symbol: str):
        async with self._lock:
            return self._data.get(symbol).get('risk_rate')

    async def set_total_lot(self, symbol: str, key: str, value: int):
        async with self._lock:
            self._data[symbol]['total_lot_b' if key == 'LONG' else 'total_lot_s'] = value

    async def get_total_lot(self, symbol: str, key: str):
        async with self._lock:
            return self._data.get(symbol).get('total_lot_b' if key == 'LONG' else 'total_lot_s')

    async def set_state(self, symbol: str, state: str):
        async with self._lock:
            self._data[symbol]['state'] = state

    async def get_state(self, symbol: str):
        async with self._lock:
            return self._data.get(symbol).get('state')

    async def update_order(self, symbol: str, data: dict):
        async with self._lock:
            self._data[symbol]['orders'].append(data)

    async def add_symbol(self, symbol: str):
        async with self._lock:
            self.symbols.append(symbol)
            self._data[symbol] = self._create_default_symbol_data()

    async def delete_symbol(self, symbol: str):
        async with self._lock:
            if symbol in self.symbols:
                self.symbols.remove(symbol)
                del self._data[symbol]

    async def get_orders(self, symbol: str):
        async with self._lock:
            return self._data.get(symbol).get('orders')

    async def del_orders(self, symbol: str, orders_id: list):
        async with self._lock:
            if orders := self._data.get(symbol).get('orders'):
                orders[:] = [order for order in orders if order['id'] not in orders_id]
