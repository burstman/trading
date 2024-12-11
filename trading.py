import pandas as pd
import ccxt
from tqdm import tqdm
import time
import random

binance = ccxt.binance()


pairs = [pair for pair in binance.load_markets() if pair.endswith("USDT")]

print("number of pairs :", len(pairs))

percentage = float(input("Enter the percentage of data you want to fetch: "))

number_of_candels_to_scan = int(input("Enter the number of candels you want to scan: "))

percentage_price_between_big_last_candel = float(
    input("Enter the percentage of price between the biggest and last candel: ")
)

print(f"Fetching {percentage}% of data...")
if percentage < 1 or percentage > 100:

    raise ValueError("Percentage must be between 1 and 100")

select_pairs = random.sample(pairs, int(len(pairs) * percentage / 100))

timeframes = ["4h"]
all_data = []

# Calculate the start date based on the number of candles we want
for timeframe in timeframes:
    print(f"Fetching data for {timeframe} timeframe...")
    for pair in tqdm(select_pairs):
        try:

            candles = binance.fetch_ohlcv(
                pair, timeframe, limit=number_of_candels_to_scan
            )

            for candle in candles:
                timestamp, open_, high, low, close, volume = candle
                all_data.append(
                    {
                        "pair": pair,
                        "timeframe": timeframe,
                        "timestamp": pd.to_datetime(timestamp, unit="ms"),
                        "open": open_,
                        "high": high,
                        "low": low,
                        "close": close,
                        "volume": volume,
                    }
                )

            time.sleep(1)  # Rate limiting

        except Exception as e:
            print(f"Error fetching data for {pair} on {timeframe}: {e}")

# Convert to DataFrame with timeframe information
df = pd.DataFrame(all_data)

# print(df)

pairs = df["pair"].unique()


def buy_or_sell_candell(id, df) -> bool:
    return df.loc[id]["close"] > df.loc[id]["open"]


for pair in pairs:
    for timeframe in timeframes:
        df_pair_timeframe = df[
            (df["pair"] == pair) & (df["timeframe"] == timeframe)
        ].copy()

        if df_pair_timeframe.empty:
            print(f"No data for pair {pair} and timeframe {timeframe}")
            continue

        df_pair_timeframe = df_pair_timeframe.reset_index(drop=True)

        # Avoid division by zero
        if (df_pair_timeframe["low"] == 0).any():
            print(f"Zero low value found for pair {pair} and timeframe {timeframe}")
            continue

        df_pair_timeframe["high_low_range"] = (
            (df_pair_timeframe["high"] - df_pair_timeframe["low"])
            * 100
            / df_pair_timeframe["low"]
        )

        # Check if high_low_range has valid data
        if df_pair_timeframe["high_low_range"].empty:
            print(f"No high_low_range data for pair {pair} and timeframe {timeframe}")
            continue

        bigger_candel = df_pair_timeframe["high_low_range"].idxmax()

        # Determines whether the "bigger candle" is a green (buy) or red (sell) candle.

        # Args:
        #     bigger_candel (int): The index of the "bigger candle" in the DataFrame.
        #     df_pair_timeframe (pandas.DataFrame): The DataFrame containing the price data for the current pair and timeframe.

        # Returns:
        #     bool: True if the "bigger candle" is a green (buy) candle, False otherwise.
        green_big_candel = buy_or_sell_candell(bigger_candel, df_pair_timeframe)

        if not green_big_candel:
            continue

        next_candels = df_pair_timeframe.loc[bigger_candel + 1 :]

        if next_candels.empty:
            # print(f"No candles after index {bigger_candel} for pair {pair}")
            continue

        price_high = df_pair_timeframe.loc[bigger_candel, "high"]
        price_low = df_pair_timeframe.loc[bigger_candel, "low"]
        price_50 = (price_high + price_low) / 2
        price_3 = (price_high + price_low) * 0.97
        price_5 = (price_high + price_low) * 0.95
        price_percent = (price_high + price_low) * (
            (100 - percentage_price_between_big_last_candel) / 100
        )

        exceeds_high = any(
            candle_high > price_high for candle_high in next_candels["high"]
        )
        below_50 = any(candle_low < price_50 for candle_low in next_candels["low"])

        last_candle_high = next_candels.tail(1)["high"].values[0]

        between_5_0 = price_5 <= last_candle_high <= price_high
        # between_30_0 = price_30 <= last_candle_high <= price_high
        between = price_percent <= last_candle_high <= price_high

        if not exceeds_high and not below_50 and between:
            print(
                f"Pair: {pair} | Timeframe: {timeframe} | Price high: {price_high} | Number of candles: {len(next_candels)}"
            )
