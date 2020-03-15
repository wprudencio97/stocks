import pandas as pd
import numpy as np
import yfinance as yf
import datetime as dt
from pandas_datareader import data as pdr
from tabulate import tabulate
import sqlite3


def main():
    stock_symbol = input('Enter a stock ticker symbol: ')
    # create the database
    create_database()
    # get the stock data
    stock_data = get_stock(stock_symbol)
    # populate the database
    populate_database(stock_symbol, stock_data)


def get_stock(stock_symbol):
    yf.pdr_override()

    start_year = 2020
    start_month = 1
    start_day = 1

    start = dt.datetime(start_year, start_month, start_day)
    now = dt.datetime.now()

    df = pdr.get_data_yahoo(stock_symbol, start, now)

    return df


def create_database():

    # Create a connection.
    conn = sqlite3.connect('stocks.db')

    # Create a cursor
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS STOCKS")
    # Create a table
    c.execute("CREATE TABLE STOCKS (" +
              "   STOCK TEXT," +
              "   DATE INTERGER, " +
              "   OPEN INTERGER, " +
              "   HIGH INTERGER, " +
              "   LOW INTERGER, " +
              "   CLOSE INTERGER, " +
              "   ADJ_ClOSE INTERGER, " +
              "   VOLUME INTERGER )"
              )
    # Save (commit) the changes
    conn.commit()

    # Close the connection.
    conn.close()


def populate_database(stock_symbol, stock_data):
    df = stock_data

    # Create a connection.
    conn = sqlite3.connect('stocks.db')

    # Create a cursor
    c = conn.cursor()

    #row_headers = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    #row_headers = "DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME"
    for i in df.index:
        # insert the values into the database
        values = (stock_symbol, str(i), str(df['Open'][i]), float(df['High']
                                                                  [i]), float(df['Low'][i]), float(df['Close'][i]), float(df['Adj Close'][i]), float(df['Volume'][i]))
        sql = "INSERT INTO STOCKS (STOCK, DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME) VALUES (?,?,?,?,?,?,?,?)"
        c.execute(sql, values)

    # Save (commit) the changes
    conn.commit()

    # Close the connection.
    conn.close()


# run the program
main()
