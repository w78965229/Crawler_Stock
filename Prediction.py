import pandas as pd
import yfinance as yf
stockNo="2330"
start_date='2022-01-01'
df =yf.download(stockNo,start=start_date)
df=df.reset_index()

