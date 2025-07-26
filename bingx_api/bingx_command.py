from asyncio import sleep
from datetime import datetime
from gzip import decompress
from time import time
from hmac import new as hmac_new
from hashlib import sha256
import websockets

import requests
from aiohttp import ClientSession
from logging import getLogger
from json import loads, dumps

from sqlalchemy.ext.asyncio import AsyncSession

from common.config import config
from common.func import add_task, get_decimal_places
from bingx_api.bingx_models import WebSocketPrice, SymbolOrderManager, TaskManager, ConfigManager, RateLimiter, \
    AccountManager
from database.orm_query import add_order, del_orders

logger = getLogger('my_app')

ws_price = WebSocketPrice()
so_manager = SymbolOrderManager()
task_manager = TaskManager()
account_manager = AccountManager()
config_manager = ConfigManager()
api_rate_limiter = RateLimiter(interval=1.0)  # Создаем экземпляр лимитера, общий для всех задач.


async def _send_request(method: str, endpoint: str, params: dict, http_session: ClientSession):
    params['timestamp'] = int(time() * 1000)
    params_str = "&".join([f"{x}={params[x]}" for x in sorted(params)])
    sign = hmac_new(config.SECRET_KEY.encode(), params_str.encode(), sha256).hexdigest()
    url = f"{config.BASE_URL}{endpoint}?{params_str}&signature={sign}"

    if http_session:  # В coin-M через aiohttp не работает
        try:
            async with http_session.request(method, url) as response:
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

        except Exception as e:
            return None, f"Ошибка при выполнении запроса send_request: {e}"

    else:
        response = requests.request(method, url, headers=config.HEADERS)
        return loads(response.text), "OK"


async def get_total_lot(symbols, http_session: ClientSession):
    for symbol in symbols:
        await sleep(1)  # Задержка перед запуском функции, иначе ошибка API

        positions_info, _ = await get_position_info(symbol, http_session)
        if positions_info := positions_info.get('data'):
            for position in positions_info:
                logger.info(
                    f'{symbol}, positionSide {position['positionSide']}, positionAmt {position["positionAmt"]}, riskRate {position["riskRate"]}')
        else:
            logger.error(f'Ошибка получения positions_info: {symbol}')


async def get_position_info(symbol: str, http_session: ClientSession):
    endpoint = '/openApi/cswap/v1/user/positions'
    params = {"symbol": f'{symbol}-USD'}

    return await _send_request("GET", endpoint, params, http_session)


async def place_order(symbol: str, side: str, executed_qty: float, tp: float, http_session: ClientSession = None):
    endpoint = '/openApi/cswap/v1/trade/order'
    take_profit_dict = {"type": "TAKE_PROFIT_MARKET", "stopPrice": tp, "workingType": "MARK_PRICE"}
    take_profit = dumps(take_profit_dict)

    params = {"symbol": f'{symbol}-USD',
              "side": "BUY" if side == "b" else "SELL",
              "positionSide": "LONG" if side == "b" else "SHORT",
              "type": "MARKET",
              "quantity": executed_qty,
              "takeProfit": take_profit,
              }

    return await _send_request("POST", endpoint, params, http_session)


async def manage_listen_key(http_session: ClientSession):
    endpoint = '/openApi/user/auth/userDataStream'

    listen_key, text = await _send_request("POST", endpoint, {}, http_session)
    if listen_key is None:
        logger.error(f'Ошибка получения listen_key: {text}')
        return

    await account_manager.add_listen_key(listen_key['listenKey'])
    while True:
        await sleep(1200)
        await _send_request("PUT", endpoint, {"listenKey": listen_key['listenKey']}, http_session)


async def transaction_upd_ws():
    while not (listen_key := await account_manager.get_listen_key()):
        await sleep(0.3)  # Задержка перед попыткой получения ключа

    channel = {}
    url = f"{config.URL_WS}?listenKey={listen_key}"

    while True:  # Цикл для повторного подключения
        try:
            async with websockets.connect(url, ping_interval=30, ping_timeout=30) as ws:
                logger.info(f"WebSocket connected transaction_upd_ws")
                await ws.send(dumps(channel))

                async for message in ws:
                    try:
                        if 'e' in (data := loads(decompress(message).decode())):
                            logger.info(f"transaction_upd_ws: {data}")

                    except Exception as e:
                        logger.error(f"Непредвиденная ошибка transaction_upd_ws: {e}, сообщение: {message}")

        except Exception as e:
            logger.error(f"Критическая ошибка transaction_upd_ws: {e}")

        logger.error(f"transaction_upd_ws завершился. Переподключение через 5 секунд.")
        await sleep(5)


@add_task(task_manager, so_manager, 'price_upd')
async def price_upd_ws(symbol, **kwargs):
    seconds = kwargs.get('seconds', 0)

    channel = {"id": '1', "reqType": "sub", "dataType": f"{symbol}-USD@markPrice"}
    await sleep(seconds)  # Задержка перед запуском функции, иначе ошибка API

    while True:
        try:
            async with websockets.connect(config.URL_WS) as ws:
                logger.error(f"WebSocket connected price_upd_ws for {symbol}")
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


async def _init_virtual_grid(symbol: str):
    current_price = await ws_price.get_price(symbol)
    decimal_places = get_decimal_places(await config_manager.get_data(symbol, 'price_step'))

    num_steps = 200
    grid_boundaries = {}

    # Получить индексы существующих ордеров
    orders = await so_manager.get_orders(symbol)
    orders_boundaries_data = {order['boundaries_index']: order['order_type'] for order in orders}
    orders_boundaries_index = orders_boundaries_data.keys()

    init_grid_step = await config_manager.get_data(symbol, 'init_grid_step')
    grid_size = await config_manager.get_data(symbol, 'grid_size')
    grid_price_upper = init_grid_step

    for i in range(num_steps):
        grid_price_lower = grid_price_upper
        grid_price_upper += grid_price_upper * grid_size

        grid_price_upper = round(grid_price_upper, decimal_places)

        # инициализировать флаги из базы (s/b, если есть такие ордера, или False)
        flag = orders_boundaries_data[i] if i in orders_boundaries_index else False
        grid_boundaries[i] = [grid_price_lower, grid_price_upper, flag]

        if grid_price_lower <= current_price < grid_price_upper:
            await so_manager.set_grid_boundaries(symbol, [i, grid_price_lower, grid_price_upper, flag])

            print(f"Цена {symbol}: {current_price}")
            print(f'текущий предел {i}: {grid_boundaries[i]}')

    print(f"Цены сетки для {symbol}: {grid_boundaries}")

    return grid_boundaries


async def _check_condition(side, index, order):
    if side == 'b':
        return index > order['boundaries_index']
    elif side == 's':
        return index < order['boundaries_index']


# Посмотреть, есть ли ордера на покупку ниже b (выше s) текущего индекса и удалить их
async def _delete_orders(symbol: str, session: AsyncSession, grid_boundaries: dict, index: int, side: str):
    orders = await so_manager.get_orders(symbol)
    orders_id, orders_boundaries_index = [], []

    for order in orders:
        if order['order_type'] == side and await _check_condition(side, index, order):
            orders_id.append(order['id'])
            orders_boundaries_index.append(order['boundaries_index'])

    if orders_id:
        await so_manager.del_orders(symbol, orders_id),
        await del_orders(session, orders_id)

        for i in orders_boundaries_index:
            grid_boundaries[i][2] = False

        price = await ws_price.get_price(symbol)
        logger.info(f"Удалено {symbol}, цена {price}: {orders_boundaries_index}")


# Посмотреть, есть ли ордера на покупку ниже b (выше s) текущего индекса и удалить их
async def _open_order(symbol: str, session: AsyncSession, grid_boundaries: dict, index: int, side: str):
    data_for_db = {
        'boundaries_index': index,
        'order_type': side,
        'open_time': datetime.fromtimestamp(int(time()))
    }

    order_id = await add_order(session, symbol, data_for_db)  # Добавить ордер в базу

    data_for_db['id'] = order_id  # Добавить id ордера в память
    await so_manager.update_order(symbol, data_for_db)  # Добавить ордер в память
    logger.info(f"Успешно добавлен {symbol}: {index}")

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


async def _handle_order_actions(symbol, session, grid_boundaries, index, price, side_old, flag, bound, side):
    await _delete_orders(symbol, session, grid_boundaries, index, side)

    if flag != side and side_old != ('s' if side == 'b' else 'b'):
        if lot := await config_manager.get_data(symbol, 'lot_b' if side == 'b' else 'lot_s'):

            # Запросить разрешение у лимитера
            await api_rate_limiter.wait_for_permission(symbol)

            data, text = await place_order(symbol, side, executed_qty=lot, tp=bound)

            if not data.get("orderId"):
                report = f'Ордер НЕ открыт {symbol}: {text} {data}\n'
                logger.error(report)

                # if data.get("code") == 100410:
                #     logger.warning('Код ошибки 100410.')
                #
                # await sleep(5)  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

                return None

            await _open_order(symbol, session, grid_boundaries, index, side)

            report = f'Ордер {symbol} по цене {price} tp {bound}: {text} {data}\n'
            logger.info(report)

        print(f"после изменения grid_boundaries: {grid_boundaries}\n")


@add_task(task_manager, so_manager, 'start_trading')
async def start_trading(symbol, **kwargs):
    session = kwargs.get('session')
    # http_session = kwargs.get('http_session')
    async_session = kwargs.get('async_session')

    async def trading_logic():
        while not await ws_price.get_price(symbol):
            await sleep(0.3)  # Задержка перед попыткой получения цены

        # logger.info(f'Запуск торговли {symbol}')

        grid_boundaries = await _init_virtual_grid(symbol)  # Инициализация сетки

        # await _delete_orders(symbol, session, grid_boundaries, index, side)
        # result = await query_leverage(symbol, http_session=http_session)  # количество открытых позиций по символу
        # logger.info(f"\n{result}\n")

        while True:
            price = await ws_price.get_price(symbol)
            index_old, lower_old, upper_old, side_old = await so_manager.get_grid_boundaries(symbol)

            # разрешаем открыть ордер при пересечении цены выше/ниже середины предыдущей grid boundaries
            await _handle_midpoint_grid_adjustment(symbol, index_old, lower_old, upper_old, side_old, price)

            if not (lower_old < price < upper_old):  # Если текущая цена ВНЕ старых границ
                for index, (lower, upper, flag) in grid_boundaries.items():

                    # Поход цены вверх в новую сетку (индекс новой сетки больше старой)
                    if lower <= price < upper and index_old < index:
                        await _handle_order_actions(symbol, session, grid_boundaries, index, price, side_old, flag,
                                                    upper, 'b')
                        await so_manager.set_grid_boundaries(symbol, [index, lower, upper, 'b'])
                        break

                    # Поход цены вниз в новую сетку (индекс новой сетки меньше старой)
                    elif lower < price <= upper and index_old > index:
                        await _handle_order_actions(symbol, session, grid_boundaries, index, price, side_old, flag,
                                                    lower, 's')
                        await so_manager.set_grid_boundaries(symbol, [index, lower, upper, 's'])
                        break

            await sleep(0.01)

    if session is None:  # Сессия не передана, создаем новый async_session_maker
        async with async_session() as session:
            await trading_logic()
    else:  # Сессия передана, используем ее (используется в хэндлерах)
        await trading_logic()
