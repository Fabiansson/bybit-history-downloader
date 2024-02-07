import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import pandas as pd
from pybit.unified_trading import HTTP
from rich import print
from rich.progress import track

timeframes = input(
    "Enter timeframes separated by comma (1,3,5,15,30,60,120,240,360,720,D,W,M): ").strip().upper().split(',')
symbols = input("Enter symbols separated by comma (e.g., BTCUSDT,ETHUSDT): ").strip().upper().split(',')
beginning_date = input("Enter beginning date (YYYY-MM-DD): ").strip()
beginning = datetime.strptime(beginning_date, "%Y-%m-%d")

CHUNK_SIZE = 20
DESTINATION_DIR = os.getcwd()
KLINE_CATEGORY = "linear"
SESSION = HTTP()


def make_chunks(lst: list, n: int) -> list:
    return [lst[i: i + n] for i in range(0, len(lst), n)]


def convert_to_seconds(interval: str) -> int:
    intervals = {
        "1": 60, "3": 180, "5": 300, "15": 900, "30": 1800, "60": 3600, "120": 7200,
        "240": 14400, "360": 21600, "720": 43200, "D": 86400, "W": 604800, "M": 2592000
    }
    if interval in intervals:
        return intervals[interval]
    else:
        raise ValueError("Invalid interval")


def generate_dates_by_minutes_limited(start_date: datetime.date, interval_minutes) -> list:
    """Generate dates at fixed intervals."""
    start_date = datetime(start_date.year, start_date.month, start_date.day)
    end_date = datetime.today()

    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += timedelta(minutes=interval_minutes)

    return date_list


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").date()
        except ValueError:
            raise ValueError("Invalid date format: {}".format(date_str))


def download_chunk(start_time: datetime, tf: str):
    """Download klines data for a specific time chunk."""
    df_tmp = pd.DataFrame(
        columns=["startTime", "openPrice", "highPrice", "lowPrice", "closePrice", "volume", "turnover"])

    for data in SESSION.get_kline(
            category=KLINE_CATEGORY,
            symbol=symbol,
            interval=tf,
            limit=1000,
            startTime=int(start_time.timestamp()) * 1000,
            endTime=start_time + timedelta(seconds=convert_to_seconds(tf))
    )["result"]["list"]:
        df_tmp.loc[len(df_tmp)] = data

    df_tmp["startTime"] = pd.to_datetime(df_tmp["startTime"].astype(float) * 1000000)
    df_tmp = df_tmp.sort_values("startTime")
    save_path = os.path.join(DESTINATION_DIR, "bybit_data", "klines", KLINE_CATEGORY, symbol, tf,
                             f"{int(start_time.timestamp())}.csv")
    df_tmp.to_csv(save_path, index=False)


def download_klines(symbol: str, beginning: datetime, tf: str):
    print(f"[bold blue]Downloading: {symbol} for timeframe:{tf}[/bold blue]")
    download_chunk(beginning, tf)

    path_first_chunk = sorted([os.path.join(directory, file) for file in os.listdir(directory)])[0]
    data_first_chunk = pd.read_csv(path_first_chunk)
    start_date_column = data_first_chunk["startTime"].iloc[0]
    # start_date = datetime.strptime(start_date_column, "%Y-%m-%d").date()
    start_date = parse_date(start_date_column)

    dates = generate_dates_by_minutes_limited(start_date, (convert_to_seconds(tf) / 60) * 1000)
    start_time_chunks = make_chunks(dates, CHUNK_SIZE)

    for start_time in start_time_chunks:
        with ThreadPoolExecutor() as executor:
            executor.map(download_chunk, start_time)
            executor.map(lambda st: download_chunk(st, tf), start_time)

    # Merge downloaded csv files
    df = pd.DataFrame(columns=["startTime", "openPrice", "highPrice", "lowPrice", "closePrice", "volume", "turnover"])
    for file in os.listdir(directory):
        df_tmp = pd.read_csv(os.path.join(directory, file))
        df = pd.concat([df, df_tmp])
        os.remove(os.path.join(directory, file))
    df = df.sort_values("startTime")
    df = df.drop_duplicates(subset=["startTime"])
    df.to_csv(os.path.join(directory, f"{tf}.csv"), index=False)


print(f"[bold blue]Downloading kline data from Bybit...[/bold blue]")

if not symbols:
    symbols = [data["symbol"] for data in SESSION.get_tickers(category=KLINE_CATEGORY)["result"]["list"]
               if data["symbol"][-4:] == "USDT"]

for symbol in track(symbols, description="Downloading klines data from Bybit"):
    for tf in timeframes:
        directory = os.path.join(DESTINATION_DIR, "bybit_data", "klines", KLINE_CATEGORY, symbol, tf)
        if not os.path.exists(directory):
            os.makedirs(directory)
        download_klines(symbol, beginning, tf)
