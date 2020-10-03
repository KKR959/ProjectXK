#=========================================================
# Description
#=========================================================
'''

'''
#=========================================================
# Imports
#=========================================================
from datetime import date
import sqlite3
import os
import yfinance as yf
import numpy as np
import code as stonkCode
import pandas as pd
import talib # for calculating parabolic Tsar and RSI - https://pypi.org/project/talib-binary/
#=========================================================
# Functions
#=========================================================
def obtainTickerSP500(config):
    table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = table[0]
    # df.to_csv('S&P500-Info.csv')
    path_to_output = config.filePaths["namesFile"]
    df.to_csv(path_to_output, columns=['Symbol', 'Security', 'GICS Sector'], index=False)
# obtainTickerSP500()

# =========================================================
def pullHistoricalData(stonk):
    stockTicker = yf.Ticker(stonk)

    data = (stockTicker.history(period="10y"))
    data.reset_index(inplace=True)
    data_dict = data.to_dict("records")
    stage_data = data_dict

    return stage_data

# =========================================================
def pullData(stonk):
    today = str(date.today())
    # for empty stocks
    stockTicker = yf.Ticker(stonk)
    #--------------------------
    try: # I had to do this horribleness b/c for some reason some stocks don't have data. should probably switch source we are pulling or something
        stockInfo = {
            'shortName':stockTicker.info['shortName'],
            'sector':stockTicker.info['sector'],
            'exchange':stockTicker.info['exchange'],
            'fullTimeEmployees':stockTicker.info['fullTimeEmployees']}
    except:
        stockInfo = {
            'shortName':"unknown",
            'sector':'Test Sector',
            'exchange':"idk",
            'fullTimeEmployees':100}
    #--------------------------
    data = (stockTicker.history(period="10y"))
    data.reset_index(inplace=True)
    data_dict = data.to_dict("records")
    #--------------------------
    return data_dict, stockInfo

#=========================================================
def initializeExchangeRegistry(config):
    # --------------------------
    # Clears out previous table
    # --------------------------
    config.databaseCursors['masterDBCursor'].execute('DROP TABLE IF EXISTS ExchangeRegistry')
    config.databaseConnectors['masterDBCon'].commit()

    # --------------------------
    # Creates the Exchange Registry table with the following three entries
    # --------------------------
    config.databaseCursors['masterDBCursor'].execute('''
            CREATE TABLE IF NOT EXISTS exchangeRegistry(
                Exchange_ID INTEGER PRIMARY KEY,
                Exchange_Name TEXT NOT NULL,
                Data_Source TEXT NOT NULL);
                ''')

    # --------------------------
    # Insert into the Exchange Registry the Information of each Exchange
    # --------------------------
    config.databaseCursors['masterDBCursor'].execute('''
            INSERT INTO exchangeRegistry(Exchange_Name, Data_Source)
            VALUES ('S&P500', 'Yahoo Finance');
            ''')

    config.databaseConnectors['masterDBCon'].commit()
# =========================================================
def initializeStockRegistry(config):
    # --------------------------
    # Creates our StockRegistry Table and populates it from our list of Stocks in the csv file
    # --------------------------

    df = pd.read_csv(config.filePaths["namesFile"])
    df.reset_index()
    df = df.rename(columns={'GICS Sector':'Sector'})
    print(df)
    # df = pd.DataFrame(data, columns=['Symbol', 'Security', 'Sector'])
    # df.reset_index()
    # print(df)



    # --------------------------
    # Creates our StockRegistry Table and populates it from our list of Stocks in the csv file
    # --------------------------
    config.databaseCursors['masterDBCursor'].execute('DROP TABLE IF EXISTS stockRegistry')
    config.databaseConnectors['masterDBCon'].commit()

    # --------------------------
    # Creates our StockRegistry Table
    # --------------------------
    config.databaseCursors['masterDBCursor'].execute(f'''
        CREATE TABLE IF NOT EXISTS stockRegistry(
            Stock_ID INTEGER PRIMARY KEY,
            Symbol nvarchar(50),
            Security nvarchar(50),
            Sector nvarchar(50));
            ''')
    # --------------------------
    # Populates StockRegistry table from our list of Stocks in the csv file
    # --------------------------
    for row in df.itertuples():
        config.databaseCursors['masterDBCursor'].execute('''
                INSERT INTO StockRegistry
                (
                Symbol, 
                Security, 
                Sector
                )
                VALUES (?,?,?)
                ''',
                    (row.Symbol,
                     row.Security,
                     row.Sector))

    config.databaseConnectors['masterDBCon'].commit()

# =========================================================
def initializeIDRegistry(config):
    # --------------------------
    # Clears out previous table
    # --------------------------
    config.databaseCursors['masterDBCursor'].execute('DROP TABLE IF EXISTS IDRegistry')
    config.databaseConnectors['masterDBCon'].commit()
    # --------------------------
    # Creates the ID Registry table with the following three entries
    # --------------------------
    config.databaseCursors['masterDBCursor'].execute('''
                        CREATE TABLE IF NOT EXISTS IDRegistry(
                            Perm_No INTEGER PRIMARY KEY,
                            Exchange_ID INTEGER NOT NULL,
                            Stock_ID INTEGER NOT NULL,
                            FOREIGN KEY (Stock_ID)
                            REFERENCES stockRegistry (Stock_ID),
                            FOREIGN KEY (EXCHANGE_ID)
                            REFERENCES exchangeRegistry (Exchange_ID)
                            );
                            ''')
    # --------------------------
    config.databaseConnectors['masterDBCon'].commit()

# =========================================================
def populateIDRegistry(config):
    # --------------------------
    # Inserts our lookup values into our IDRegistry Lookup Table
    # --------------------------

    # --------------------------
    # This code needs to be updated to not use nested for loops, can use null values as temp values to get around this for loop
    # --------------------------
    config.databaseCursors['masterDBCursor'].execute('''SELECT * FROM exchangeRegistry''')
    exchange_records = config.databaseCursors['masterDBCursor'].fetchall()
    for row in exchange_records:
        exchange_id_temp = row[0]
        config.databaseCursors['masterDBCursor'].execute('''SELECT * FROM stockRegistry''')
        stock_records = config.databaseCursors['masterDBCursor'].fetchall()
        for row in stock_records:
            stock_id_temp = row[0]
            config.databaseCursors['masterDBCursor'].execute('''
                    INSERT INTO IDRegistry(Exchange_ID, Stock_ID)
                     VALUES (?,?)''', (exchange_id_temp, stock_id_temp))

    config.databaseConnectors['masterDBCon'].commit()

# =========================================================
def initializeStageTable(config):
    # --------------------------
    # Drop our Staging Table if needed
    # --------------------------
    # config.databaseCursors['masterDBCursor'].execute('''DROP TABLE IF EXISTS StdStageTable''')
    # config.databaseConnectors['masterDBCon'].commit()

    config.databaseCursors['masterDBCursor'].execute('''DROP TABLE IF EXISTS StageTable''')
    config.databaseConnectors['masterDBCon'].commit()

    # config.databaseCursors['masterDBCursor'].execute('''DROP TABLE IF EXISTS IDStageTable''')
    # config.databaseConnectors['masterDBCon'].commit()

    # --------------------------
    # Delete data in our staging tables if needed
    # --------------------------
    # config.databaseCursors['masterDBCursor'].execute('''
    #         DELETE FROM StdStageTable''')

    # config.databaseCursors['masterDBCursor'].execute('''
    #         DELETE FROM StageTable''')

    # --------------------------
    # Create our StdRegistry Staging Table
    # --------------------------
    # config.databaseCursors['masterDBCursor'].execute('''
    #             CREATE TABLE IF NOT EXISTS StdStageTable(
    #                 Perm_No INTEGER PRIMARY KEY,
    #                 St_Dev INTEGER
    #                 );
    #                 ''')
    #
    # config.databaseConnectors['masterDBCon'].commit()

    # --------------------------
    # Create our DataRegistry Staging Table
    # --------------------------
    config.databaseCursors['masterDBCursor'].execute('''
            CREATE TABLE IF NOT EXISTS StageTable(
                Data_ID INTEGER PRIMARY KEY,
                Perm_No INTEGER NOT NULL,
                Date DATE NOT NULL,
                Open DOUBLE,
                High DOUBLE,
                Low DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Dividends DOUBLE,
                Stock_Splits DOUBLE,
                SAR DOUBLE,
                RSI DOUBLE,
                memPred DOUBLE,
                linReg DOUBLE,
                ranForestPred DOUBLE
                );
                ''')

    config.databaseConnectors['masterDBCon'].commit()

# =========================================================
def initializeDataRegistry(config):
    # --------------------------
    # Drop our DataRegistry Table If needed
    # --------------------------
    config.databaseCursors['masterDBCursor'].execute('''DROP TABLE IF EXISTS DataRegistry''')
    config.databaseConnectors['masterDBCon'].commit()

    # --------------------------
    # Create our DataRegistry Facts Table
    # --------------------------
    config.databaseCursors['masterDBCursor'].execute('''
            CREATE TABLE IF NOT EXISTS DataRegistry (
                Data_ID INTEGER PRIMARY KEY,
                Perm_No INTEGER,
                Date DATE NOT NULL,
                Open DOUBLE,
                High DOUBLE,
                Low DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Dividends DOUBLE,
                Stock_Splits DOUBLE,
                SAR DOUBLE,
                RSI DOUBLE,
                memPred DOUBLE,
                linReg DOUBLE,
                ranForestPred DOUBLE,
                FOREIGN KEY (Perm_No)
                REFERENCES IDRegistry (Perm_No)
                );
                ''')

    # --------------------------
    # Create a non clustered unique index on Perm_no and date for lookup purposes
    # May suggest switching the order of the nix to stock date then perm no
    # --------------------------
    config.databaseCursors['masterDBCursor'].execute('''
            CREATE UNIQUE INDEX nix_permno_date ON DataRegistry (Perm_No, Date)
            ''')

    config.databaseConnectors['masterDBCon'].commit()

# =========================================================
def populateDataRegistry(config):
    # --------------------------
    # Loop through our list of Companies tickers and pull data using YFinance
    # --------------------------
    config.nameListGen(config)
    print(config.nameList)

    for symbol in config.nameList:
        # --------------------------
        # Get stock data for each ticker
        # --------------------------
        # print(symbol)
        data = pullHistoricalData(symbol)
        # print(data)
        # --------------------------
        # Turn into pandas frame
        # --------------------------
        # df = pd.DataFrame(data)

        # --------------------------
        # The following code is an alternative way to process the data, keep commented out
        # --------------------------
        processedData = []
        for idx, k in enumerate(data):
            # Format in Date, Open, High, Low, Close, Volume, Dividends, Stock Split, RSI, SAR
            hold = [str(k['Date']), str(k['Open']), str(k['High']), str(k['Low']), str(k['Close']), str(k['Volume']),
                    str(k['Dividends']), str(k['Stock Splits'])]
            processedData.append(hold)
        df = pd.DataFrame(processedData,
                          columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Split'])

        # --------------------------
        # SAR Calc
        # --------------------------
        df['SAR'] = talib.SAR(df['High'], df['Low'], acceleration=0.02, maximum=0.2)
        # --------------------------
        # RSI
        # --------------------------
        df['RSI'] = talib.RSI(df["Close"], config.RSILookbackTime)
        # print(df)


        # --------------------------
        # Find the correct Perm_No for the current stock we are searching data on.
        # --------------------------
        found_value = config.databaseCursors['masterDBCursor'].execute('''
                            SELECT Perm_No
                            From IDRegistry ID
                                INNER JOIN stockRegistry SR
                                ON ID.Stock_ID = SR.Stock_ID
                            WHERE Symbol = ?''', (symbol,)).fetchall()
        # print(found_value)
        found_value_list = found_value[0]
        # print(found_value_list)

        print("Stock#:", found_value_list[0])
        print(df)

        # --------------------------
        # Load in Data
        # --------------------------
        # df_dict = df.to_dict("records")
        # stage_data = df_dict
        # print(stage_data)

        # print(df)

        # for k in stage_data:
        for idx in range(len(df)):
            # --------------------------
            # The following code is apart of the alternative way to process the data, keep commented out
            # --------------------------
            values = (
                found_value_list[0], df.at[idx, 'Date'], df.at[idx, 'Open'], df.at[idx, 'High'], df.at[idx, 'Low'], df.at[idx, 'Close'],
                df.at[idx, 'Volume'], df.at[idx, 'Dividends'], df.at[idx, 'Stock Split'], df.at[idx, 'SAR'],
                df.at[idx, 'RSI'])

            # --------------------------
            # Populate our staging table where we can modify any data needed before loading it into our permanent Date
            # Registry Table
            # --------------------------
            config.databaseCursors['masterDBCursor'].execute('''
                                    INSERT INTO StageTable
                                        (
                                        Perm_No,
                                        Date,
                                        Open,
                                        High,
                                        Low,
                                        Close,
                                        Volume,
                                        Dividends,
                                        Stock_Splits,
                                        SAR,
                                        RSI
                                        )
                                    VALUES (?,?,?,?,?,?,?,?,?,?,?)''', values)

        config.databaseConnectors['masterDBCon'].commit()

    # # --------------------------
    # # Insert our data from our Data Staging Table into our Data Registry Facts Table
    # # --------------------------
    config.databaseCursors['masterDBCursor'].execute('''
            INSERT OR REPLACE INTO DataRegistry
            (
                Data_ID,
                Perm_No,
                Date,
                Open,
                High,
                Low,
                Close,
                Volume,
                Dividends,
                Stock_Splits,
                SAR,
                RSI
            )
            SELECT
                    ST.Data_ID,
                    ST.Perm_No,
                    ST.Date,
                    ST.Open,
                    ST.High,
                    ST.Low,
                    ST.Close,
                    ST.Volume,
                    ST.Dividends,
                    ST.Stock_Splits,
                    ST.SAR,
                    ST.RSI
            FROM StageTable ST
                 LEFT JOIN DataRegistry DR
                 ON DR.Perm_No = ST.Perm_No
                 AND DR.Date = ST.Date
            WHERE DR.Data_ID IS NULL
            ''')

    # --------------------------
    # Clear our data staging table for next use
    # --------------------------
    config.databaseCursors['masterDBCursor'].execute('''
            DELETE FROM StageTable''')

    config.databaseConnectors['masterDBCon'].commit()

# =========================================================
def initializeCalculationsRegistry(config):
    # --------------------------
    # Drop Table if Needed
    # --------------------------
    config.databaseCursors['masterDBCursor'].execute('''DROP TABLE IF EXISTS CalculationsRegistry''')
    config.databaseConnectors['masterDBCon'].commit()

    # --------------------------
    # Create the standard deviation table and connect it to our IDRegistry
    # --------------------------
    config.databaseCursors['masterDBCursor'].execute('''
                CREATE TABLE IF NOT EXISTS CalculationsRegistry(
                    Perm_No INTEGER PRIMARY KEY,
                    StandardDeviation DOUBLE,
                    ValueScore DOUBLE,
                    LongValue INTEGER,
                    ShortValue INTEGER,
                    EvolScore DOUBLE,
                    LinRegScore DOUBLE,
                    MemoryScore DOUBLE,
                    RanForestScore Double
                    );
                    ''')
    config.databaseConnectors['masterDBCon'].commit()
# =========================================================
def performInitialCalculations(config):
    #=========================================================
    # Populating the standard deviation field
    #=========================================================
    # config.nameListGen(config)
    # print(config.nameList)

    for symbol in config.nameList:
        #--------------------------
        codes = config.pullStockData(config, "masterDBCursor", symbol, "Low", "High")
        values = []
        print(codes)
        for k in codes:
            values.append([k[0], k[1]])
        standardDeviation = np.std(values)

        # --------------------------
        # Find the correct Perm_No for the current stock we are searching data on.
        # --------------------------
        found_value = config.databaseCursors['masterDBCursor'].execute('''
                                    SELECT Perm_No
                                    From IDRegistry ID
                                        INNER JOIN stockRegistry SR
                                        ON ID.Stock_ID = SR.Stock_ID
                                    WHERE Symbol = ?''', (symbol,)).fetchall()
        # print(found_value)
        found_value_list = found_value[0]
        # print(found_value)
        # --------------------------
        # Insert into the CalculationsRegistry table
        # --------------------------
        config.databaseCursors['masterDBCursor'].execute(f'''
                                    INSERT OR REPLACE INTO CalculationsRegistry
                                    (
                                    Perm_No,
                                    StandardDeviation
                                    )
                                    VALUES (?,?)''', (int(found_value_list[0]), standardDeviation,))

    # --------------------------
    config.databaseConnectors['masterDBCon'].commit()
# =========================================================

# =========================================================
def initializeStockDatabase(config):
    # obtainTickerSP500(config)
    initializeExchangeRegistry(config)
    initializeStockRegistry(config)
    initializeIDRegistry(config)
    populateIDRegistry(config)
    initializeStageTable(config)
    initializeDataRegistry(config)
    populateDataRegistry(config)
    initializeCalculationsRegistry(config)
    performInitialCalculations(config)

# =========================================================
# Sar Calc | https://blog.quantinsti.com/parabolic-sar/
# RSI | https://stackoverflow.com/questions/57006437/calculate-rsi-indicator-from-pandas-dataframe
