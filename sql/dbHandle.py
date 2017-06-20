import re
import sqlite3
import os
import numpy as np
import csv
#sqlite3.register_adapter(np.float64, float)
#sqlite3.register_adapter(np.float32, float)
#sqlite3.register_adapter(np.int64, int)
#sqlite3.register_adapter(np.int32, int) 


def resetTable(cur):
    # Do some setup
    cur.executescript('''
	DROP TABLE IF EXISTS DailyTrades;
    DROP TABLE IF EXISTS Brokerages;
    DROP TABLE IF EXISTS Stocks;
    DROP TABLE IF EXISTS Dates;
	DROP TABLE IF EXISTS BuySell;
	

    CREATE TABLE DailyTrades (
        id      INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        brokerage_id    INTEGER,
        stock_id INTEGER,
		date_id	INTEGER,
        price REAL,
        volume INTEGER,
        buysell_id INTEGER
    );

    CREATE TABLE Brokerages (
        id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        symbol TEXT UNIQUE,
		name TEXT UNIQUE
    );    

    CREATE TABLE Stocks (
        id   INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
		symbol TEXT UNIQUE,
        name TEXT UNIQUE
    ); 
	
	CREATE TABLE Dates (
        id   INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
		date TEXT UNIQUE
    ); 
	
	CREATE TABLE BuySell (
        id   INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
		name TEXT UNIQUE
    );	
    
    ''')
    
    
class dbHandle():
    con_db = None
    cur_db = None
    dbname = None
    dirs = None
    
    def __init__(self, dbname):
        print("Initial Database connection..." + dbname)
        self.dirs = os.listdir('.')
        self.dbname = dbname
        self.con_db = sqlite3.connect(dbname)
        self.con_db.text_factory = str
        self.cur_db = self.con_db.cursor()
        self.con_db.commit()
        
    def __del__(self):
        self.cur_db.close()
	
    def _insertGetID(self, cur, tablename, fieldname, value):
        ft = cur.execute('SELECT id FROM ' + tablename +  ' WHERE ' + fieldname + ' = (?)',(value, )).fetchone()
        if ft is None:
            cur.execute('INSERT OR IGNORE INTO ' + tablename + ' (' + fieldname + ') VALUES (?)',(value, ))
            return cur.execute('SELECT id FROM ' + tablename +  ' WHERE ' + fieldname + ' = (?)',(value, )).fetchone()[0]
        else:
            return ft[0]
    
    def insertDB(self):
        for d in self.dirs:
            if len(d.split('_'))!=2:
                continue
            
            date = d.split('_')[1]
            if date.isdigit():
                print('Processing Date... ' + str(date))
                filenames = os.listdir('./' + d)
                for fname in filenames:
                    self.insert_csv2DB('./' + d + '/' + fname)
                self.con_db.commit()
    def insert_csv2DB(self, filename):
        fs = filename.split('_')
        
        try:
            date = fs[1].split('/')[0]
            stock_symbol = re.sub('.+/','',filename)[0:4]
            stock_name = re.sub('[0-9]+/[0-9]+','',filename.split('_')[1])
            brokerage_position = 'NA'
            
            f = open(filename, 'r', encoding = 'utf8', errors='ignore')
            for row in csv.reader(f):
                if row[0]=='':
                    return True
                brokerage_symbol = row[1][0:4]
                brokerage_name = row[1][4:len(row[1])].replace(' ','').replace('\u3000','')
                price = row[2]
                
                if row[3]!='0':
                    volume = row[3]
                    buysell = 'buy'
                else:
                    volume = row[4]
                    buysell = 'sell'
                
                #brokerage

                ft = self.cur_db.execute('SELECT id FROM Brokerages WHERE symbol = ? LIMIT 1', \
                                   ( brokerage_symbol,) ).fetchone()
                if ft is None:
                    self.cur_db.execute('INSERT OR IGNORE INTO Brokerages (symbol, name) VALUES ( ?, ?)', \
                                       (brokerage_symbol, brokerage_name) )
                    self.cur_db.execute('SELECT id FROM Brokerages WHERE symbol = ? AND name = ? LIMIT 1', \
                                       ( brokerage_symbol, brokerage_name) )
                    brokerage_id = self.cur_db.fetchone()[0]
                else:
                    brokerage_id = ft[0]
                
                
                #stock
                ft = self.cur_db.execute('SELECT id FROM Stocks WHERE symbol = ? LIMIT 1', \
                                   (stock_symbol,) ).fetchone()
                if ft is None:
                    self.cur_db.execute('INSERT OR IGNORE INTO Stocks (symbol, name) VALUES ( ?, ? )', \
                                       (stock_symbol, stock_name) )  
                    self.cur_db.execute('SELECT id FROM Stocks WHERE symbol = ? AND name = ? LIMIT 1', \
                                       (stock_symbol, stock_name) ) 
                    stock_id = self.cur_db.fetchone()[0]
                else:
                    stock_id = ft[0]
                
                #date
                date_id = self._insertGetID(self.cur_db, 'Dates', 'date', date)
                
                #buysell
                buysell_id = self._insertGetID(self.cur_db, 'BuySell', 'name', buysell)
                
                #DailyTrades
                self.cur_db.execute( \
                'INSERT OR IGNORE INTO DailyTrades (brokerage_id, stock_id, date_id, price, volume, buysell_id) VALUES ( ?, ?, ?, ?, ?, ? )', \
                                   (brokerage_id, stock_id, date_id, price, volume, buysell_id) ) 
        except:
            print('error!')
            print('date: ' + str(date))
            print('filename: ' + filename)
            print('stock symbol: ' + stock_symbol)
            print('stock name: ' + stock_name)
            print('row: ' + str(row))
            print('brokerage_symbol: ' + brokerage_symbol)
            print('brokerage_name: ' + brokerage_name)
            print('volume: ' + volume)
            print('buysell: ' + buysell)
            
        