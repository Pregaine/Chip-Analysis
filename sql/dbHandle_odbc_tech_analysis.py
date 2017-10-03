# -*- coding: utf-8 -*-

import pyodbc
import re
import os
import csv
import re
from datetime import datetime
import pandas as pd

def resetTable( cur ):
    # Do some setup
    with cur.execute( '''
    DROP TABLE IF EXISTS DailyTechAnalysis;
    '''
    ):
        print( 'Successfuly Del all Table' )

def CreateDailyTechAnalysis( cur ):

    with cur.execute( '''
    CREATE TABLE dbo.DailyTechAnalysis
	(
        id int NOT NULL IDENTITY (1, 1),
        stock_id int,
        date_id int,
        open_price real,
        high real,
        low  real,
        close_price real, 
        volume real,
        daily_gain real,
        week_gain real,
        month_gain real,
        MA3 real,
        MA5 real,
        MA8 real,
        MA10 real,
        MA20 real,
        MA120 real,
        MA240 real,
        MA480 real,
        MA720 real,
        RSI12 real,
        MACD_DIF real,
        DEM real,
        OSC real,
        MFI_6 real,
        MFI_14 real,
        WILLR_9 real,
        WILLR_18 real,
        WILLR_56 real,
        PLUS_DI real,
        MINUS_DI real,
        DX real,
        ADX real,
        Upperband real,
        Middleband real,
        Dnperband real,
        BB real,
        W20 real,
        _20_Bias real,
        _60_Bias real,
        kdj_k real,
        kdj_d real
	)  ON [PRIMARY]
    
    ALTER TABLE dbo.DailyTechAnalysis ADD CONSTRAINT
	PK_DailyTechAnalysis PRIMARY KEY CLUSTERED 
	(
	    id
	) WITH( STATISTICS_NORECOMPUTE = OFF, 
	IGNORE_DUP_KEY = OFF, 
	ALLOW_ROW_LOCKS = ON, 
	ALLOW_PAGE_LOCKS = ON ) ON [PRIMARY]

    ALTER TABLE dbo.DailyTechAnalysis SET (LOCK_ESCALATION = TABLE)
    
    COMMIT
    '''
    ):
        print( 'Successfuly Create Daily Trade')

class dbHandle(  ):

    con_db = None
    cur_db = None
    dbname = None
    dirs = None

    def __init__( self, server, database, username, password ):

        print( "Initial Database connection..." + database )
        self.dbname = database
        self.con_db = pyodbc.connect( 'DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server +
                                      ';PORT=1443;DATABASE=' + database +
                                      ';UID=' + username +
                                      ';PWD=' + password )

        self.cur_db = self.con_db.cursor( )

        self.con_db.commit( )

    def _insertGetID( self, cur, tablename, fieldname, value ):

        ft = cur.execute( 'SELECT id FROM ' + tablename + ' WHERE ' + fieldname + ' = (?)', (value,) ).fetchone( )

        if ft is None:
            cur.execute( 'INSERT INTO ' + tablename + ' (' + fieldname + ') VALUES (?)', (value,) )
            return cur.execute(
                'SELECT id FROM ' + tablename + ' WHERE ' + fieldname + ' = (?)', (value,) ).fetchone( )[ 0 ]
        else:
            return ft[ 0 ]

    def insert_csv2DB( self, filename ):

        df = pd.read_csv( filename, sep = ',', encoding = 'utf8', na_values = None )
        df[ '日期' ] = pd.to_datetime( df[ '日期' ], format = "%Y/%m/%d" )

        df = df.astype( object ).where( pd.notnull( df ), None )

        stock_symbol = filename[ 2:6 ]

        for index, row in df.iterrows( ):

            stock_id = self.cur_db.execute(
                'SELECT TOP 1 id FROM Stocks WHERE symbol = ?', ( stock_symbol,) ).fetchone( )[ 0 ]

            # date
            date_id = self._insertGetID( self.cur_db, 'Dates', 'date', row[ '日期' ] )

            # DailyTrades
            self.cur_db.execute(
                '''INSERT INTO DailyTechAnalysis ( \
                date_id,
                stock_id,
                close_price,
                open_price,
                high,
                low,
                volume,
                daily_gain,
                MA3
                ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ? )''',
                ( date_id,
                  stock_id,
                  row[ '收盤' ],
                  row[ '開盤' ],
                  row[ '最高' ],
                  row[ '最低' ],
                  row[ '成交量' ],
                  row[ '日漲幅' ],
                  row[ 'MA03']
                   ) )

    def insertDB( self, src_str ):

        for d in os.listdir( '.\\' ):

            if d.endswith( src_str + ".csv" ):
                print( d )
                self.insert_csv2DB( '.\\' + d )
                self.con_db.commit( )


def main( ):
    start = datetime.now( )
    server   = 'localhost'
    database = 'StockDB'
    username = 'sa'
    password = 'admin'

    db = dbHandle( server, database, username, password )

    resetTable( db.cur_db ) # 這句會刪掉SQL內所有資料
    CreateDailyTechAnalysis( db.cur_db )

    db.insertDB( '日線技術指標' )

    print( datetime.now( ) - start )

if __name__ == '__main__':
    main( )