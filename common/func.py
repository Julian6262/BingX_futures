from asyncio import create_task
from functools import wraps


def add_task(outer_func, so_manager, text: str):
    def decorator(func):
        @wraps(func)
        async def wrapper(symbol, *args, **kwargs):
            if await so_manager.get_state(symbol) == 'stop':
                print(f'Отслеживание {text} {symbol} не запущено, state = STOP')
                return

            task = create_task(func(symbol, *args, **kwargs))
            await outer_func.add_task(symbol, task)
            print(f'Запущено отслеживание {text} {symbol}')
            return task

        return wrapper

    return decorator
