import pandas as pd
import sys
import twint
import nest_asyncio
import os
nest_asyncio.apply()

if len(sys.argv) == 1:
    print('s&p500')
    ticker_names = pd.read_csv('/Users/dragonlook/Dropbox (MIT)/Private/System_Trade/Codes/MOTHER_DRAGON/DRAGON/data/Stock/new_list_500.csv',index_col=0).iloc[:,0].to_list()
else:
    # print(sys.argv[0])
    ticker_names = sys.argv[1]

# c = twint.Config()
os.makedirs('STOCK_sentiment',exist_ok=True)

for tick in ticker_names[113:]:
    if len(tick) == 1:
        continue
    c = twint.Config()
    # c.Username = "DailyHotStocks"
    c.Search = '#' + tick
    c.Since = "2020-06-08"
    # c.Limit = 10  # Not working.
    c.Store_csv = True
    c.Output = "STOCK_sentiment/" + tick +'.csv'

    twint.run.Search(c)