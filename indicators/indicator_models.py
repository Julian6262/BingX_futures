# from asyncio import sleep
# from collections import deque
# from logging import getLogger
# from aiohttp import ClientSession
# from talib import MACD
# from numpy import array as np_array
#
# from bingx_api.bingx_command import get_candlestick_data, ws_price, so_manager, task_manager, config_manager
# from common.func import add_task
#
# logger = getLogger('my_app')
#
#
# async def _get_initial_close_prices(symbol: str, http_session: ClientSession, interval: str, limit: int = 300):
#     data, text = await get_candlestick_data(symbol, http_session, interval, limit=limit)
#
#     if not (data_ok := data.get("data")):
#         logger.error(f'Ошибка получения данных candlestick {symbol}: {data}, {text}')
#         return None
#
#     open_times, close_price = zip(*[(item['time'], item['close']) for item in reversed(data_ok)])
#
#     timeframe_minutes = {'30m': 30, '1h': 60}
#
#     delta = timeframe_minutes[interval] * 60 * 1000 - 1
#     next_candle_time = open_times[-1] + delta
#     return delta, next_candle_time, deque(close_price, maxlen=limit)
#
#
# async def _process_indicators_logic(symbol: str, close_prices: deque, logic_name: str):
#     close_prices = np_array(close_prices, dtype=float)
#
#     match logic_name:
#         case 'macd_30m':
#             _, _, hist = MACD(close_prices, fastperiod=12, slowperiod=26, signalperiod=9)
#
#             if hist[-2] >= 0:
#                 # logger.info(f'\n{symbol} buy')
#                 await so_manager.set_b_s_trigger(symbol, 'b')
#
#             elif hist[-2] < 0:
#                 # logger.info(f'\n{symbol} sell')
#                 await so_manager.set_b_s_trigger(symbol, 's')
#
#
# @add_task(task_manager, so_manager, 'start_indicators')
# async def start_indicators(symbol: str, http_session: ClientSession):
#     while not await ws_price.get_price(symbol):
#         await sleep(0.3)  # Задержка перед попыткой получения цены
#
#     if not (initial_1h_data := await _get_initial_close_prices(symbol, http_session, '30m')):
#         return
#
#     delta_1h, next_candle_time_1h, close_prices_deque_1h = initial_1h_data
#     await _process_indicators_logic(symbol, close_prices_deque_1h, 'macd_30m')
#     await config_manager.set_data(symbol, 'macd', True)  # сначала индикатор, потом запуск торгов
#
#     logger.info(f'Запуск start_indicators {symbol}')
#
#     while True:
#         time_now, price = await ws_price.get_price(symbol)
#
#         if time_now >= next_candle_time_1h:
#             close_prices_deque_1h[-1] = price
#             close_prices_deque_1h.append(price)
#             next_candle_time_1h += delta_1h  # Обновляем время следующей свечи
#             await _process_indicators_logic(symbol, close_prices_deque_1h, 'macd_30m')
#
#         await sleep(1)
