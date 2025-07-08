from asyncio import sleep
from decimal import Decimal
from datetime import datetime
from gzip import decompress
from time import time
from hmac import new as hmac_new
from hashlib import sha256

import requests
from aiohttp import ClientSession, ClientConnectorError
from logging import getLogger
from json import loads, JSONDecodeError

from common.config import config
from common.func import add_task
from bingx_api.bingx_models import WebSocketPrice, SymbolOrderManager, AccountManager, TaskManager, ConfigManager
from database.orm_query import add_order, del_orders

logger = getLogger('my_app')

ws_price = WebSocketPrice()
so_manager = SymbolOrderManager()
account_manager = AccountManager()
task_manager = TaskManager()
config_manager = ConfigManager()


async def _send_request111(method: str, session: ClientSession, endpoint: str, params: dict):
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
        async with session.post(url) as response:
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


async def manage_listen_key(http_session: ClientSession):
    endpoint = '/openApi/user/auth/userDataStream'  # OK

    listen_key, text = await _send_request("POST", http_session, endpoint, {})
    if listen_key is None:
        logger.error(f'Ошибка получения listen_key: {text}')
        return

    await account_manager.add_listen_key(listen_key['listenKey'])
    while True:
        await sleep(1200)
        await _send_request("PUT", http_session, endpoint, {"listenKey": listen_key['listenKey']})


async def account_upd_ws(http_session: ClientSession):
    while not (listen_key := await account_manager.get_listen_key()):
        await sleep(0.3)  # Задержка перед попыткой получения ключа

    channel = {"id": "1", "reqType": "sub", "dataType": "ACCOUNT_UPDATE"}  # NO OK !!!!!!!!!!!!!!
    url = f"{config.URL_WS}?listenKey={await account_manager.get_listen_key()}"

    while True:  # Цикл для повторного подключения
        try:
            async with http_session.ws_connect(url) as ws:
                logger.info(f"WebSocket connected account_upd_ws")
                await ws.send_json(channel)

                async for message in ws:
                    try:
                        if 'e' in (data := loads(decompress(message.data).decode())):
                            await account_manager.update_balance_batch(data['a']['B'])
                            logger.info(f"Account_upd_ws: {data['a']}")

                    except Exception as e:
                        logger.error(f"Непредвиденная ошибка account_upd_ws: {e}, сообщение: {message.data}")

        except Exception as e:
            logger.error(f"Критическая ошибка account_upd_ws: {e}")

        logger.error(f"account_upd_ws завершился. Переподключение через 5 секунд.")
        await sleep(5)


@add_task(task_manager, so_manager, 'price_upd')
async def price_upd_ws(symbol, **kwargs):
    seconds = kwargs.get('seconds', 0)
    http_session = kwargs.get('http_session')

    channel = {"id": '1', "reqType": "sub", "dataType": f"{symbol}-USD@markPrice"}
    await sleep(seconds)  # Задержка перед запуском функции, иначе ошибка API

    while True:
        try:
            async with http_session.ws_connect(config.URL_WS) as ws:
                # logger.info(f"WebSocket connected price_upd_ws for {symbol}")
                await ws.send_json(channel)

                async for message in ws:
                    try:
                        if 'data' in (data := loads(decompress(message.data).decode())):
                            await ws_price.update_price(symbol, int(time() * 1000), float(data["data"]["p"]))

                    except Exception as e:
                        logger.error(f"Непредвиденная ошибка price_upd_ws: {e}, сообщение: {message.data}")

        except Exception as e:
            logger.error(f"Критическая ошибка price_upd_ws: {symbol}, {e}")

        # logger.error(f"price_upd_ws для {symbol} завершился. Переподключение через 5 секунд.")
        await sleep(5)  # Пауза перед повторным подключением


async def _init_virtual_grid(symbol: str, num_steps: int, step_size: float, init_grid_step: float):
    _, current_price = await ws_price.get_price(symbol)

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
            await so_manager.set_grid_boundaries(symbol, [i, grid_price_lower, grid_price_upper, flag])

            print(f"Цена {symbol}: {current_price}")
            print(f'текущий предел {i}: {grid_boundaries[i]}')

    print(f"Цены сетки для {symbol}: {grid_boundaries}")

    return grid_boundaries


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
            _, current_price = await ws_price.get_price(symbol)
            index_old, lower_old, upper_old, side_old = await so_manager.get_grid_boundaries(symbol)


            if not (lower_old < current_price < upper_old):
                # orders = await so_manager.get_orders(symbol)
                # orders_boundaries = {order['boundaries_index']: (order['order_type'], order['id']) for order in orders}

                for index, (lower, upper, flag) in grid_boundaries.items():
                    if lower <= current_price < upper and index_old < index:  # Поход цены вверх
                        print(f"\nДо изменения grid_boundaries: {grid_boundaries}")
                        print(f'index_old {index_old}, index {index}, {lower}, {upper}, {flag}')

                        if not flag and side_old != 's':
                            # tp_price = upper - abs((upper - lower)) * 0.1
                            tp_price = upper

                            # if await place_order(symbol, "buy", upper, tp_price)

                            logger.info(f"Покупка {grid_boundaries[index]} по цене {current_price}, TP: {tp_price}")

                            data_for_db = {
                                'boundaries_index': index,
                                'order_type': 'b',
                                'open_time': datetime.fromtimestamp(int(time()))
                            }

                            # orders = await so_manager.get_orders(symbol)
                            # orders_data = [(order['boundaries_index'], order['id']) for order in orders if
                            #                order['order_type'] == 'b' and order['boundaries_index'] < index]
                            #
                            # orders_id = [order[1] for order in orders_data]
                            # orders_boundaries_index = [order[0] for order in orders_data]

                            # Посмотреть, есть ли ордера на покупку ниже текущего индекса и удалить их
                            orders = await so_manager.get_orders(symbol)
                            orders_id, orders_boundaries_index = [], []

                            for order in orders:
                                if order['order_type'] == 'b' and index > order['boundaries_index']:
                                    orders_id.append(order['id'])
                                    orders_boundaries_index.append(order['boundaries_index'])

                            print(f"\norders_data покупка(id, index): {orders_id, orders_boundaries_index}\n")

                            await so_manager.del_orders(symbol, orders_id),
                            await del_orders(symbol, session, orders_id)
                            for index2 in orders_boundaries_index:
                                grid_boundaries[index2][2] = False

                            order_id = await add_order(session, symbol, data_for_db)  # Добавить ордер в базу
                            data_for_db['id'] = order_id  # Добавить id ордера в память
                            await so_manager.update_order(symbol, data_for_db)  # Добавить ордер в память
                            grid_boundaries[index][2] = 'b'
                            # orders_boundaries[index] = ('b', order_id)

                        # for i in range(num_steps):
                        #     flag = orders_boundaries_index[i] if i in orders_boundaries_index.keys() else False
                        #     grid_boundaries[i][2] = flag  # Переписать все флаги с учетом открытых ордеров

                        await so_manager.set_grid_boundaries(symbol, [index, lower, upper, 'b'])

                        print(f"после изменения grid_boundaries: {grid_boundaries}\n")

                        # grid_boundaries[index - 1][2] = True  # Предыдущий шаг не должен быть активным
                        break


                    elif lower < current_price <= upper and index_old > index:  # Поход цены вниз
                        print(f"\nДо изменения grid_boundaries: {grid_boundaries}")
                        print(f'index_old {index_old}, index {index}, {lower}, {upper}, {flag}')

                        if not flag and side_old != 'b':
                            # tp_price = lower + abs((upper - lower)) * 0.1
                            tp_price = lower

                            # if await place_order(symbol, "sell", lower, tp_price)

                            logger.info(f"Продажа {grid_boundaries[index]} по цене {current_price}, TP: {tp_price}")

                            data_for_db = {
                                'boundaries_index': index,
                                'order_type': 's',
                                'open_time': datetime.fromtimestamp(int(time()))
                            }

                            orders = await so_manager.get_orders(symbol)
                            orders_id, orders_boundaries_index = [], []

                            for order in orders:
                                if order['order_type'] == 's' and index < order['boundaries_index']:
                                    orders_id.append(order['id'])
                                    orders_boundaries_index.append(order['boundaries_index'])

                            print(f"\norders_data продажа(id, index): {orders_id, orders_boundaries_index}\n")

                            await so_manager.del_orders(symbol, orders_id),
                            await del_orders(symbol, session, orders_id)
                            for index2 in orders_boundaries_index:
                                grid_boundaries[index2][2] = False

                            order_id = await add_order(session, symbol, data_for_db)  # Добавить ордер в базу
                            data_for_db['id'] = order_id  # Добавить id ордера в память
                            await so_manager.update_order(symbol, data_for_db)  # Добавить ордер в память
                            grid_boundaries[index][2] = 's'
                            # orders_boundaries[index] = ('s', order_id)

                        # for i in range(num_steps):
                        #     flag = orders_boundaries_index[i] if i in orders_boundaries_index.keys() else False
                        #     grid_boundaries[i][2] = flag  # Переписать все флаги с учетом открытых ордеров

                        await so_manager.set_grid_boundaries(symbol, [index, lower, upper, 's'])

                        print(f"после изменения grid_boundaries: {grid_boundaries}\n")

                        # grid_boundaries[index + 1][2] = True  # Предыдущий шаг не должен быть активным
                        break

            await sleep(0.01)

    if session is None:  # Сессия не передана, создаем новый async_session_maker
        async with async_session() as session:
            await trading_logic()
    else:  # Сессия передана, используем ее (используется в хэндлерах)
        await trading_logic()
