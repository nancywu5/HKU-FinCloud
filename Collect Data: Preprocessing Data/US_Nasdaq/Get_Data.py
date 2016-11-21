from yahoo_finance import Share
import pandas as pd
import csv
import os.path

def EditCsv(stock):
	data = Share(stock)
	x = data.get_historical('2000-01-01','2016-04-01')
	df = pd.DataFrame(x)
	cols = ['Date','Open','High','Low','Close','Volume','Adj_Close']
	df = df.ix[:,cols]
	df = df.set_index('Date')
	df.to_csv("Stock/"+stock+".csv")
def get_list(stock_list_file):
	stock_list = []
	with open(stock_list_file, 'rb') as csvfile:
		spamreader = csv.reader(csvfile, delimiter=',')
		for row in spamreader:
			stock_list.append(row)
	return stock_list[0]

if __name__ == "__main__":
	stock_list = get_list('0.Stock_Ticker.csv')
	for stock in stock_list:
		if not os.path.isfile("Stock/"+stock+".csv"):
			try:
				EditCsv(stock)
				print ("Finished",stock)
			except:
				print (stock+"cannot download")
