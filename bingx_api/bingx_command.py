from asyncio import sleep
from decimal import Decimal
from datetime import datetime
from gzip import decompress
from time import time
from hmac import new as hmac_new
from hashlib import sha256
import websockets

import requests
from aiohttp import ClientSession, ClientConnectorError
from logging import getLogger
from json import loads, dumps, JSONDecodeError

from sqlalchemy.ext.asyncio import AsyncSession

from common.config import config
from common.func import add_task
from bingx_api.bingx_models import WebSocketPrice, SymbolOrderManager, TaskManager, ConfigManager
from database.orm_query import add_order, del_orders

logger = getLogger('my_app')

ws_price = WebSocketPrice()
so_manager = SymbolOrderManager()
task_manager = TaskManager()
config_manager = ConfigManager()


async def _send_request(method: str, session: ClientSession, endpoint: str, params: dict):
    params['timestamp'] = int(time() * 1000)
    params_str = "&".join([f"{x}={params[x]}" for x in sorted(params)])
    sign = hmac_new(config.SECRET_KEY.encode(), params_str.encode(), sha256).hexdigest()
    url = f"{config.BASE_URL}{endpoint}?{params_str}&signature={sign}"

    # headers = {
    #     'X-BX-APIKEY': config.API_KEY,
    # }
    #
    # response = requests.request(method, url, headers=headers, data={})
    # return loads(response.text), "OK"
    try:
        async with session.request(method, url) as response:
            if response.status == 200:
                if response.content_type == 'application/json':
                    data = await response.json()
                elif response.content_type == 'text/plain':
                    data = loads(await response.text())
                else:
                    return None, f"Неожиданный Content-Type send_request: {response.content_type}"

                return data, "OK"

            else:
                return None, f"Ошибка {response.status} для {params.get('symbol')}: {await response.text()}"

    except ClientConnectorError as e:
        return None, f'Ошибка соединения с сетью (send_request): {e}'

    except JSONDecodeError as e:
        return None, f"Ошибка декодирования send_request JSON: {e}"

    except Exception as e:
        return None, f"Ошибка при выполнении запроса send_request: {e}"


async def place_order(symbol: str, session: ClientSession, side: str, executed_qty: float | Decimal):
    endpoint = '/openApi/cswap/v1/trade/order'
    params = {"symbol": f'{symbol}-USD', "type": "MARKET", "side": side, "quantity": executed_qty}

    return await _send_request("POST", session, endpoint, params)


@add_task(task_manager, so_manager, 'price_upd')
async def price_upd_ws(symbol, **kwargs):
    seconds = kwargs.get('seconds', 0)

    channel = {"id": '1', "reqType": "sub", "dataType": f"{symbol}-USD@markPrice"}
    await sleep(seconds)  # Задержка перед запуском функции, иначе ошибка API

    while True:
        try:
            async with websockets.connect(config.URL_WS) as ws:
                logger.info(f"WebSocket connected price_upd_ws for {symbol}")
                await ws.send(dumps(channel))

                async for message in ws:
                    try:
                        if 'data' in (data := loads(decompress(message).decode())):
                            await ws_price.update_price(symbol, float(data["data"]["p"]))

                    except Exception as e:
                        logger.error(f"Непредвиденная ошибка price_upd_ws: {e}, сообщение: {message}")

        except Exception as e:
            logger.error(f"Критическая ошибка price_upd_ws: {symbol}, {e}")

        logger.error(f"price_upd_ws для {symbol} завершился. Переподключение через 5 секунд.")
        await sleep(5)  # Пауза перед повторным подключением


async def _init_virtual_grid(symbol: str, num_steps: int, step_size: float, init_grid_step: float):
    current_price = await ws_price.get_price(symbol)

    half_n = num_steps // 2
    grid_boundaries = {}

    orders = await so_manager.get_orders(symbol)
    orders_boundaries_data = {order['boundaries_index']: order['order_type'] for order in orders}
    orders_boundaries_index = orders_boundaries_data.keys()

    for i in range(num_steps):
        grid_price_upper = init_grid_step + step_size * (i - half_n)
        grid_price_lower = grid_price_upper - step_size

        # инициализировать флаги из базы (s/b, если есть такие ордера, или False)
        flag = orders_boundaries_data[i] if i in orders_boundaries_index else False
        grid_boundaries[i] = [grid_price_lower, grid_price_upper, flag]

        if grid_price_lower <= current_price < grid_price_upper:
            # index = i
            # await _delete_orders(symbol, async_session, grid_boundaries, i, 'b')
            await so_manager.set_grid_boundaries(symbol, [i, grid_price_lower, grid_price_upper, flag])

            print(f"Цена {symbol}: {current_price}")
            print(f'текущий предел {i}: {grid_boundaries[i]}')

    print(f"Цены сетки для {symbol}: {grid_boundaries}")

    return grid_boundaries


def _check_condition(side, index, order):
    if side == 'b':
        return index > order['boundaries_index']
    elif side == 's':
        return index < order['boundaries_index']


# Посмотреть, есть ли ордера на покупку ниже b (выше s) текущего индекса и удалить их
async def _delete_orders(symbol: str, session: AsyncSession, grid_boundaries: dict, index: int, side: str):
    orders = await so_manager.get_orders(symbol)
    orders_id, orders_boundaries_index = [], []

    for order in orders:
        if order['order_type'] == side and _check_condition(side, index, order):
            orders_id.append(order['id'])
            orders_boundaries_index.append(order['boundaries_index'])

    if orders_id:
        logger.info(f'\nПопытка закрыть ордер: {orders_id, orders_boundaries_index}\n')

        await so_manager.del_orders(symbol, orders_id),
        await del_orders(session, orders_id)

        for i in orders_boundaries_index:
            grid_boundaries[i][2] = False

        logger.info(f"\nУдалено, {side} (id, index): {orders_id, orders_boundaries_index}\n")


# Посмотреть, есть ли ордера на покупку ниже b (выше s) текущего индекса и удалить их
async def _open_order(symbol: str, session: AsyncSession, grid_boundaries: dict, index: int, side: str):
    data_for_db = {
        'boundaries_index': index,
        'order_type': side,
        'open_time': datetime.fromtimestamp(int(time()))
    }

    order_id = await add_order(session, symbol, data_for_db)  # Добавить ордер в базу
    logger.info(f"\nУспешно добавлен в базу index {index}\n")

    data_for_db['id'] = order_id  # Добавить id ордера в память
    await so_manager.update_order(symbol, data_for_db)  # Добавить ордер в память
    logger.info(f"\nУспешно добавлен в память index, order_id: {index, order_id}\n")

    grid_boundaries[index][2] = side


async def _handle_midpoint_grid_adjustment(symbol: str, index_old, lower_old, upper_old, side_old, current_price):
    midpoint = (lower_old + upper_old) / 2
    log_message_prefix = None

    if side_old == 'b' and current_price >= midpoint:
        log_message_prefix = "b"
    elif side_old == 's' and current_price <= midpoint:
        log_message_prefix = "s"

    if log_message_prefix:
        await so_manager.set_grid_boundaries(symbol, [index_old, lower_old, upper_old, False])
        # logger.info(f"{log_message_prefix} индекс {index_old} середина, цена {current_price}\n")


async def _handle_order_actions(symbol, session, grid_boundaries, index, price, side_old, tp, flag, side):
    await _delete_orders(symbol, session, grid_boundaries, index, side)

    if flag != side and side_old != ('s' if side == 'b' else 'b'):
        # if await place_order(symbol, "buy", upper, tp_price)
        await _open_order(symbol, session, grid_boundaries, index, side)
        logger.info(f"{side} {index}, {grid_boundaries[index]} по цене {price}, TP: {tp}")

        print(f"после изменения grid_boundaries: {grid_boundaries}\n")


@add_task(task_manager, so_manager, 'start_trading')
async def start_trading(symbol, **kwargs):
    session = kwargs.get('session')
    http_session = kwargs.get('http_session')
    async_session = kwargs.get('async_session')

    async def trading_logic():
        while not await ws_price.get_price(symbol):
            await sleep(0.3)  # Задержка перед попыткой получения цены

        logger.info(f'Запуск торговли {symbol}')

        num_steps = 40

        init_grid_step = await config_manager.get_data(symbol, 'init_grid_step')
        step_size = init_grid_step * await config_manager.get_data(symbol, 'grid_size')  # grid size в %

        grid_boundaries = await _init_virtual_grid(symbol, num_steps, step_size, init_grid_step)

        while True:
            price = await ws_price.get_price(symbol)
            index_old, lower_old, upper_old, side_old = await so_manager.get_grid_boundaries(symbol)

            # разрешаем открыть ордер при пересечении цены выше/ниже середины предыдущей grid boundaries
            await _handle_midpoint_grid_adjustment(symbol, index_old, lower_old, upper_old, side_old, price)

            if not (lower_old < price < upper_old):  # Если текущая цена ВНЕ старых границ
                for index, (lower, upper, flag) in grid_boundaries.items():

                    # Поход цены вверх в новую сетку (индекс новой сетки больше старой)
                    if lower <= price < upper and index_old < index:
                        tp_price = upper
                        # tp_price = upper - abs((upper - lower)) * 0.1
                        await _handle_order_actions(symbol, session, grid_boundaries, index, price, side_old, tp_price,
                                                    flag, 'b')

                        await so_manager.set_grid_boundaries(symbol, [index, lower, upper, 'b'])
                        break

                    # Поход цены вниз в новую сетку (индекс новой сетки меньше старой)
                    elif lower < price <= upper and index_old > index:
                        tp_price = lower
                        # tp_price = lower + abs((upper - lower)) * 0.1
                        await _handle_order_actions(symbol, session, grid_boundaries, index, price, side_old, tp_price,
                                                    flag, 's')

                        await so_manager.set_grid_boundaries(symbol, [index, lower, upper, 's'])
                        break

            await sleep(0.01)

    if session is None:  # Сессия не передана, создаем новый async_session_maker
        async with async_session() as session:
            await trading_logic()
    else:  # Сессия передана, используем ее (используется в хэндлерах)
        await trading_logic()
