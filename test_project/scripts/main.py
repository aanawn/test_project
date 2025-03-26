import requests
import psycopg2
from psycopg2 import sql


def get_coins():
    cmc_url = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing"
    cmc_params = {"start": 1, "limit": 1500, "sortBy": "market_cap", "sortType": "desc"}
    cmc_coins = {c['symbol'].upper() for c in
                 requests.get(cmc_url, params=cmc_params).json()['data']['cryptoCurrencyList']}

    sw_url = "https://simpleswap.io/api/v3/currencies"
    sw_params = {"fixed": "false", "includeDisabled": "false"}
    sw_coins = {c['symbol'].upper() for c in requests.get(sw_url, params=sw_params).json()}

    return cmc_coins - sw_coins


def save_to_db(missing_coins):
    conn = psycopg2.connect(
        dbname="test_task",
        user="crypto_user",
        password="12345",
        host="localhost"
    )

    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS coins (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        for coin in missing_coins:
            cursor.execute(
                sql.SQL("INSERT INTO coins (symbol) VALUES (%s) ON CONFLICT (symbol) DO NOTHING"),
                (coin,)
            )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    save_to_db(get_coins())