import asyncio
import ast

from aiogram import Bot, types, Dispatcher, executor

from mongo import main


bot = Bot("6238822686:AAEVBZ-TPyKfOcWlqsSyk8_5eroPOzTylvQ")
dp = Dispatcher(bot=bot)

@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    await message.answer(f'Привет!!!')


@dp.message_handler()
async def listen(message: types.Message):
    try:
        dictionary = ast.literal_eval(message.text)
        res = await main(dictionary.get('dt_from'), dictionary.get('dt_upto'), dictionary.get('group_type'))
    except Exception:
        res = 'Отправьте json нужного формата'
    await message.answer(res)

if  __name__ == '__main__':
    executor.start_polling(dp)

