from asyncio import gather

from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiohttp import ClientSession
from sqlalchemy.ext.asyncio import AsyncSession

from bingx_api.bingx_command import so_manager, task_manager, config_manager, price_upd_ws, start_trading
from common.config import config
from database.orm_query import del_symbol, add_symbol, update_state
from filters.chat_types import IsAdmin

router = Router()
router.message.filter(IsAdmin(config.ADMIN))  # Фильтр по ID, кто может пользоваться ботом


@router.message(F.text.startswith('track_') | F.text.startswith('pause_') | F.text.startswith('stop_'))
async def set_state_cmd(message: Message, session: AsyncSession, http_session: ClientSession):
    state_new, symbol = message.text.split('_')
    symbol = symbol.upper()

    if symbol not in so_manager.symbols:
        return await message.answer('Не такой символ')

    state_old = await so_manager.get_state(symbol)

    await gather(
        update_state(session, symbol, state_new),
        so_manager.set_state(symbol, state_new)
    )

    if state_old in ('track', 'pause') and state_new == 'stop':
        await task_manager.del_tasks(symbol)
        # await so_manager.set_b_s_trigger(symbol, 'new')

    elif state_old == 'stop' and state_new in ('track', 'pause'):
        await gather(
            price_upd_ws(symbol, http_session=http_session),
            # start_indicators(symbol, http_session=http_session),
            start_trading(symbol, session=session, http_session=http_session)
        )

    await message.answer(f"Статус монеты {symbol} изменен c {state_old} на {state_new}")


@router.message(F.text.startswith('addf_'))  # Добавить символ в БД
async def add_symbol_cmd(message: Message, session: AsyncSession, http_session: ClientSession):
    if (symbol := message.text[5:].upper()) not in config_manager.symbols:
        return await message.answer('Не такой символ')

    if symbol in so_manager.symbols:
        return await message.answer('Данный символ уже существует')

    # price = await get_price(symbol, http_session)
    # price = float(price[0]['data'][0]['markPrice'])

    # await message.answer(str(price))

    await gather(
        add_symbol(symbol, session),
        so_manager.add_symbol(symbol),
    )
    await message.answer('Символ добавлен в статусе "stop"')


@router.message(F.text.startswith('delf_'))  # Удалить символ из БД
async def del_symbol_cmd(message: Message, session: AsyncSession):
    if (symbol := message.text[5:].upper()) not in so_manager.symbols:
        return await message.answer('Не такой символ')

    if await so_manager.get_state(symbol) != 'stop':
        return await message.answer('Сначала переведите в статус "stop"')

    if await so_manager.get_orders(symbol):
        return await message.answer('По данному символу есть ордера')

    if await so_manager.get_profit(symbol):
        return await message.answer('По данному символу есть профит')

    await gather(
        del_symbol(symbol, session),
        so_manager.delete_symbol(symbol),
    )

    await message.answer('Символ удален')


# ----------------- T E S T ---------------------------------------
@router.message(CommandStart())
async def start_cmd(message: Message, session: AsyncSession, http_session: ClientSession):
    for tasks in task_manager._tasks.items():
        print(tasks)
    for tasks in so_manager._data.items():
        print(tasks)

    # data_for_db = {
    #     'boundaries_index': 1,
    #     'order_type': 's',
    #     'open_time': datetime.fromtimestamp(int(time()))
    # }
    #
    # order_id = await add_order(session, "TONCOIN", data_for_db)  # Добавить ордер в базу
    # logger.info(f"\nУспешно добавлен в базу index {index}")

    # async def get_current_orders(symbol: str, session: ClientSession):
    #     endpoint = '/openApi/cswap/v1/user/positions'
    #     params = {"symbol": f'{symbol}-USD'}
    #
    #     return await _send_request("GET", session, endpoint, params)

    # async def place_order(symbol: str, session: ClientSession):
    #     endpoint = '/openApi/cswap/v1/trade/order'
    #     params = {
    #         "symbol": "TONCOIN-USD",
    #         "side": "BUY",
    #         "positionSide": "LONG",
    #         "type": "MARKET",
    #         "quantity": 1,
    #         "takeProfit": "{\"type\": \"TAKE_PROFIT_MARKET\", \"stopPrice\": 2.7584,\"workingType\":\"MARK_PRICE\"}"
    #     }
    #
    #     return await _send_request111("POST", session, endpoint, params)

    # response = await place_order('TONCOIN', http_session)
    # await message.answer(str(response))

    # import time
    # import requests
    # import hmac
    # from hashlib import sha256
    #
    # APIURL = "https://open-api.bingx.com"
    # APIKEY = config.API_KEY
    # SECRETKEY = config.SECRET_KEY
    #
    # async def demo():
    #     payload = {}
    #     path = '/openApi/cswap/v1/trade/order'
    #     method = "POST"
    #     paramsMap = {
    #         "symbol": "TONCOIN-USD",
    #         "side": "BUY",
    #         "positionSide": "LONG",
    #         "type": "MARKET",
    #         "quantity": 1,
    #         "takeProfit": "{\"type\": \"TAKE_PROFIT_MARKET\", \"stopPrice\": 2.7584,\"workingType\":\"MARK_PRICE\"}"
    #     }
    #     paramsStr = await parseParam(paramsMap)
    #     return await send_request(method, path, paramsStr, payload)
    #
    # async def get_sign(api_secret, payload):
    #     signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()
    #     print("sign=" + signature)
    #     return signature
    #
    # async def send_request(method, path, urlpa, payload):
    #     url = "%s%s?%s&signature=%s" % (APIURL, path, urlpa, await get_sign(SECRETKEY, urlpa))
    #     print(url)
    #     headers = {
    #         'X-BX-APIKEY': APIKEY,
    #     }
    #     response = requests.request(method, url, headers=headers, data=payload)
    #     return response.text
    #
    # async def parseParam(paramsMap):
    #     sortedKeys = sorted(paramsMap)
    #     paramsStr = "&".join(["%s=%s" % (x, paramsMap[x]) for x in sortedKeys])
    #     if paramsStr != "":
    #         return paramsStr + "&timestamp=" + str(int(time.time() * 1000))
    #     else:
    #         return paramsStr + "timestamp=" + str(int(time.time() * 1000))
    #
    # print(await demo())
