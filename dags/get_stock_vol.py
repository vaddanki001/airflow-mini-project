import yfinance as yf
from datetime import date, timedelta

start_date = date.today()
end_date = start_date + timedelta(days=1)
tsla_df = yf.download('TSLA', start="2021-03-01", end="2021-03-02", interval='1m')
stock_symbol = 'TSLA'.lower()
file_name = 'data_'+ stock_symbol+'.csv'
tsla_df.to_csv(file_name,  header=False)
