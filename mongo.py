import asyncio
from datetime import datetime, timedelta
import json

import motor.motor_asyncio
from dateutil.relativedelta import relativedelta


async def fn(dt_from: str = "", dt_upto: str = '', group_type: str = ''):
    if group_type == "day":
        format_date = "%Y-%m-%d"
    elif group_type == "month":
        format_date = "%Y-%m"
    elif group_type == "hour":
        format_date = "%Y-%m-%d-%H"
    else:
        return  "Неверный параметр даты"
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
    db = client['sampleDB']
    collection = db['sample_collection']
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)
    result = await collection.aggregate([{"$match": {'dt': {'$gte': dt_from, "$lte": dt_upto}}},
                                             {"$group": {
                                                 "_id": {"$dateToString": {"format": format_date, "date": "$dt"}},
                                                 "count": {"$sum": "$value"}}},
                                             {"$sort": {"_id": 1}}]).to_list(None)
    if group_type == "day":
        count_day = (dt_upto - dt_from).days + 1
        dataset = [0 for i in range(count_day)]
        labels = [datetime.isoformat(dt_from + timedelta(days=i)) for i in range(count_day)]
        for i in result:
            date = datetime.strptime(i["_id"], format_date)
            index = (date - dt_from).days
            dataset[index] = i['count']

    elif group_type == "month":
        count_month = ((dt_upto.year - dt_from.year) * 12) + (dt_upto.month - dt_from.month) + 1
        dataset = [0 for i in range(count_month)]
        labels = [datetime.isoformat(dt_from + relativedelta(months=+i)) for i in range(count_month)]
        for i in result:
            date = datetime.strptime(i["_id"], format_date)
            index = ((date.year - dt_from.year) * 12) + (date.month - dt_from.month)
            dataset[index] = i['count']

    elif group_type == "hour":
        count_hour = ((dt_upto - dt_from).days *24) + (dt_upto.hour - dt_from.hour) + 1
        dataset = [0 for i in range(count_hour)]
        labels = [datetime.isoformat(dt_from + relativedelta(hours=+i)) for i in range(count_hour)]
        for i in result:
            date = datetime.strptime(i["_id"], format_date)
            index = ((date - dt_from).days * 24) + (date.hour - dt_from.hour)
            dataset[index] = i['count']
    else:
        return "Что то пошло не так"

    response = json.dumps({"dataset": dataset, "labels":labels})
    return response

async def main(gte, lte, group_type):
    task = asyncio.create_task(fn(gte, lte, group_type))
    await task
    return task.result()





