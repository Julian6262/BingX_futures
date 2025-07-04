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

# from indicators.indicator_models import start_indicators

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

    # profit = await so_manager.get_profit('ADA')
    # await message.answer(f'profit ADA {profit}')
    # profit = await so_manager.get_profit('TRX')
    # await message.answer(f'profit TRX {profit}')
    # profit = await so_manager.get_profit('XRP')
    # await message.answer(f'profit XRP {profit}')
    # sum_profit = await so_manager.get_summary_profit()
    # await message.answer(f'sum_profit {sum_profit}')

    # endpoint = '/openApi/cswap/v1/trade/order'
    #
    # params = {"symbol": 'ADA-USD',
    #           "type": "TRIGGER_MARKET",
    #           "side": "SELL",
    #           "quantity": 1,
    #           # "price": 0.5845,
    #           "positionSide": "SHORT",
    #           "stopPrice": 0.5,
    #           # "takeProfit": "{\"type\": \"TAKE_PROFIT_MARKET\", \"stopPrice\": 31968.0,\"price\": 31968.0,\"workingType\":\"MARK_PRICE\"}"
    #           }
    #
    # report = await send_request("POST", http_session, endpoint, params)
    # print(report)
    # await message.answer(report)



