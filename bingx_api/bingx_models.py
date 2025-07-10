from asyncio import Lock, CancelledError
from collections import defaultdict


class ConfigManager:
    def __init__(self):
        self.symbols = []
        self._data = defaultdict(dict)
        self._lock = Lock()

    async def load_config(self, batch_data: dict):
        async with self._lock:
            for data in batch_data:
                self.symbols.append(data.symbol_name)
                self._data[data.symbol_name]['init_grid_step'] = data.init_grid_step
                self._data[data.symbol_name]['grid_size'] = data.grid_size
                self._data[data.symbol_name]['lot'] = data.lot

    # async def set_data(self, symbol: str, key: str, value: float | bool):
    #     async with self._lock:
    #         self._data[symbol][key] = value

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

    async def update_price(self, symbol: str, price: float):
        async with self._lock:
            self._data[symbol] = price

    async def get_price(self, symbol: str):
        async with self._lock:
            return self._data.get(symbol)


class SymbolOrderManager:  # Класс для работы с ордерами в реальном времени
    def __init__(self):
        self.symbols = []
        self._data = defaultdict(self._create_default_symbol_data)
        self._lock = Lock()

    @staticmethod
    def _create_default_symbol_data():
        return {'state': 'stop',
                'profit': 0.0,
                'orders': []}

    async def add_symbols_and_orders(self, batch_data: list):
        async with self._lock:
            for symbol, orders in batch_data:
                self.symbols.append(symbol.name)
                self._data[symbol.name]['state'] = symbol.state
                self._data[symbol.name]['profit'] = symbol.profit
                self._data[symbol.name]['orders'] = orders

    async def set_grid_boundaries(self, symbol: str, grid_boundarie: list):
        async with self._lock:
            self._data[symbol]['grid_boundarie'] = grid_boundarie

    async def get_grid_boundaries(self, symbol: str):
        async with self._lock:
            return self._data.get(symbol).get('grid_boundarie')

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

    async def update_profit(self, symbol: str, profit: float):
        async with self._lock:
            self._data[symbol]['profit'] += profit

    async def get_profit(self, symbol: str):
        async with self._lock:
            return self._data.get(symbol).get('profit')

    async def get_summary_profit(self):
        async with self._lock:
            return sum(symbol_data['profit'] for _, symbol_data in self._data.items())

    async def del_orders(self, symbol: str, orders_id: list):
        async with self._lock:
            if orders := self._data.get(symbol).get('orders'):
                orders[:] = [order for order in orders if order['id'] not in orders_id]
