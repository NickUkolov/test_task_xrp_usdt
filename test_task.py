import time

from pymongo import MongoClient
import requests
from datetime import datetime, timedelta

key = "https://api.binance.com/api/v3/ticker/price?symbol=XRPUSDT"

uri = 'mongodb://root:admin@localhost:27017/admin?authSource=admin&authMechanism=SCRAM-SHA-1'


def database(url):
    """
    Подключение к базе данных

    :param url: str
    :return: Collection
    """
    client = MongoClient(url)
    db = client.store_db
    transactions = db.transactions
    return transactions


def get_and_write_in_db_binance_data(url, db_transaction):
    """
    Получение цены валюты с бинанс и сохранение в базу данных с таймкодом

    :param url: str
    :param db_transaction: Collection
    :return: Response
    """
    response = requests.get(url)
    data = response.json()
    data_dict = {
        'price': float(data['price']),
        'time': datetime.now()
    }
    db_transaction.insert_one(data_dict)
    return response


def clear_data_to_hour_span(db_transaction):
    """
    Поддержание постоянного объема записей (в течение одного часа)
    в базе данных с целью экономии ресурсов и для сравнения цены в промежутке часа
    согласно условию тестового

    :param db_transaction: Collection
    :return: None
    """
    time_for_del = datetime.now() - timedelta(minutes=60)
    db_transaction.delete_many({'time': {"$lt": time_for_del}})


def find_hour_diff_in_price(db_transaction):
    """
    Сравнение макс и мин значений цены в течение часа
    и выведение сообщения в консоль при условии, что цена за последний час
    упала на 1%
    (при необходимости без print можно вывести f строкой в return)

    :param db_transaction: Collection
    :return: None
    """
    transaction = db_transaction.aggregate([
        {"$group": {
            "_id": {},
            "max_price": {"$max": "$price"},
            "min_price": {"$min": "$price"}
        }}
    ])
    max_and_min = [el for el in transaction][0]
    if max_and_min['min_price'] <= (max_and_min['max_price'] * 0.99):
        print('Цена упала на 1%')


def main(binance_url, mongo_url):
    """
    Главная функция плюс условия для того, чтобы избежать
    бана на бирже исходя из лимитов АПИ, т.к. при угрозе бана
    отправляется в ответ заголовок Retry-After и/или код ответа
    429/418

    :param binance_url: str
    :param mongo_url: str
    :return: None
    """
    db = database(mongo_url)
    while True:
        response = get_and_write_in_db_binance_data(
            url=binance_url,
            db_transaction=db
        )
        clear_data_to_hour_span(db_transaction=db)
        find_hour_diff_in_price(db_transaction=db)
        if response.headers.get('Retry-After'):
            time.sleep(int(response.headers.get('Retry-After')))
            print('Слишком много запросов, угроза бана')
        if response.status_code == (429 | 418):
            time.sleep(60)


if __name__ == '__main__':
    main(binance_url=key, mongo_url=uri)
