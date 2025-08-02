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
api_rate_limiter = RateLimiter(interval=2.0)  # Создаем экземпляр лимитера, общий для всех задач.


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


async def _handle_position_info(symbol: str, positions_info):
    for position in positions_info:
        await so_manager.set_total_lot(symbol, position['positionSide'], int(position["positionAmt"]))
        await so_manager.set_risk_rate(symbol, float(position["riskRate"]))


async def request_total_lot(symbols, http_session: ClientSession):
    for symbol in symbols:
        await sleep(1)  # Задержка перед запуском функции, иначе ошибка API

        # Сделать в цикле !!!!!!!!!

        positions_info, _ = await get_position_info(symbol, http_session)
        if 'data' in positions_info:
            positions_info = positions_info['data']

            if positions_info is None:
                logger.info(f'Нет открытых позиций: {symbol}, {positions_info}')
                await so_manager.set_risk_rate(symbol, 0.0)
                return

            await _handle_position_info(symbol, positions_info)

        else:
            logger.error(f'Ошибка получения positions_info, пауза 5 сек: {symbol}, {positions_info}')
            await sleep(5)

            positions_info, _ = await get_position_info(symbol, http_session)
            if 'data' in positions_info:
                positions_info = positions_info['data']

                if positions_info is None:
                    logger.info(f'Нет открытых позиций: {symbol}, {positions_info}')
                    await so_manager.set_risk_rate(symbol, 0.0)
                    return

                await _handle_position_info(symbol, positions_info)

            else:
                logger.error(f'Ошибка получения positions_info, пауза 5 сек: {symbol}, {positions_info}')

        logger.info(f'total загружен: {symbol}')


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

    while True:
        try:
            async with websockets.connect(url, ping_interval=30, ping_timeout=30) as ws:
                print(f"WebSocket connected transaction_upd_ws")
                await ws.send(dumps(channel))

                async for message in ws:
                    try:
                        if 'o' in (data := loads(decompress(message).decode())):
                            await _handle_total_lot(data['o'])

                    except Exception as e:
                        logger.error(f"Непредвиденная ошибка transaction_upd_ws: {e}, сообщение: {message}")

        except Exception as e:
            print(f"Критическая ошибка transaction_upd_ws: {e}")

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
                print(f"WebSocket connected price_upd_ws for {symbol}")
                await ws.send(dumps(channel))

                async for message in ws:
                    try:
                        if 'data' in (data := loads(decompress(message).decode())):
                            await ws_price.update_price(symbol, float(data["data"]["p"]))

                    except Exception as e:
                        logger.error(f"Непредвиденная ошибка price_upd_ws: {e}, сообщение: {message}")

        except Exception as e:
            print(f"Критическая ошибка price_upd_ws: {symbol}, {e}")

        logger.error(f"price_upd_ws для {symbol} завершился. Переподключение через 5 секунд.")
        await sleep(5)  # Пауза перед повторным подключением


async def _handle_total_lot(data: dict):
    if data['s']:
        # logger.info(f"transaction_upd_ws: {data}")

        symbol = data['s'].split('-')[0]
        quantity = int((data['q'].split('.')[0])[:-1])
        total_lot_b = await so_manager.get_total_lot(symbol, 'LONG')
        total_lot_s = await so_manager.get_total_lot(symbol, 'SHORT')

        if data['S'] == 'BUY' and data['ps'] == 'LONG':
            logger.info(f"{symbol}: LONG + {quantity}: lot_b {total_lot_b + quantity} lot_s {total_lot_s}")
            await so_manager.set_total_lot(symbol, 'LONG', total_lot_b + quantity)

        elif data['S'] == 'SELL' and data['ps'] == 'SHORT':
            logger.info(f"{symbol}: SHORT + {quantity}: lot_b {total_lot_b} lot_s {total_lot_s + quantity}")
            await so_manager.set_total_lot(symbol, 'SHORT', total_lot_s + quantity)

        elif data['S'] == 'SELL' and data['ps'] == 'LONG':
            logger.info(f"{symbol}: LONG - {quantity}: lot_b {total_lot_b - quantity} lot_s {total_lot_s}")
            await so_manager.set_total_lot(symbol, 'LONG', total_lot_b - quantity)

        elif data['S'] == 'BUY' and data['ps'] == 'SHORT':
            logger.info(f"{symbol}: SHORT - {quantity}: lot_b {total_lot_b} lot_s {total_lot_s - quantity}")
            await so_manager.set_total_lot(symbol, 'SHORT', total_lot_s - quantity)


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


async def _manage_total_lot(symbol: str, side: str, lot: int):
    total_lot_b = await so_manager.get_total_lot(symbol, 'LONG')
    total_lot_s = await so_manager.get_total_lot(symbol, 'SHORT')
    dynamic_lot = lot

    # report = f'total_lot_b {total_lot_b} total_lot_s {total_lot_s}'
    # logger.info(report)

    if side == 'b':

        if 3 * lot <= total_lot_s - total_lot_b <= 10 * lot:
            dynamic_lot = lot * 2

        elif total_lot_s - total_lot_b > 10 * lot:
            dynamic_lot = lot * 3

        # report = f'total_lot_s - total_lot_b {total_lot_s - total_lot_b} dinamic_lot {dynamic_lot}'
        # logger.info(report)

    elif side == 's':

        if 3 * lot <= total_lot_b - total_lot_s <= 10 * lot:
            dynamic_lot = lot * 2

        elif total_lot_b - total_lot_s > 10 * lot:
            dynamic_lot = lot * 3

        # report = f'total_lot_b - total_lot_s {total_lot_b - total_lot_s} dinamic_lot {dynamic_lot}'
        # logger.info(report)

    return dynamic_lot


async def _handle_order_actions(symbol, session, grid_boundaries, index, price, side_old, flag, bound, side):
    await _delete_orders(symbol, session, grid_boundaries, index, side)

    if flag != side and side_old != ('s' if side == 'b' else 'b'):
        if lot := await config_manager.get_data(symbol, 'lot_b' if side == 'b' else 'lot_s'):
            dynamic_lot = await _manage_total_lot(symbol, side, lot)
            price_step = await config_manager.get_data(symbol, 'price_step')
            decimal_places = get_decimal_places(await config_manager.get_data(symbol, 'price_step'))
            tp = round(bound + (-price_step if side == 'b' else price_step), decimal_places)

            # Запросить разрешение у лимитера
            await api_rate_limiter.wait_for_permission(symbol)

            data, text = await place_order(symbol, side, executed_qty=dynamic_lot, tp=tp)

            if not data.get("orderId"):
                report = f'Ордер НЕ открыт {symbol}: {text} {data}\n'
                logger.error(report)

                # if data.get("code") == 100410:
                #     logger.warning('Код ошибки 100410.')
                #
                # await sleep(5)  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

                return None

            await _open_order(symbol, session, grid_boundaries, index, side)

            report = f'Ордер {symbol} цена {price} tp {tp} dinamic_lot {dynamic_lot}: {data}\n'
            logger.info(report)

        print(f"после изменения grid_boundaries: {grid_boundaries}\n")


@add_task(task_manager, so_manager, 'start_trading')
async def start_trading(symbol, **kwargs):
    session = kwargs.get('session')
    # http_session = kwargs.get('http_session')
    async_session = kwargs.get('async_session')

    async def trading_logic():
        while not ((await so_manager.get_risk_rate(symbol))[1] and await ws_price.get_price(symbol)):
            await sleep(0.3)

        logger.info(f'Запуск торговли {symbol}')

        grid_boundaries = await _init_virtual_grid(symbol)  # Инициализация сетки

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
