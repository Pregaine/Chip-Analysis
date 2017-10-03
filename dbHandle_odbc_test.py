# -*- coding: utf-8 -*-

import pyodbc
import re
import os
import csv
from datetime import datetime

def resetTable( cur ):
    # Do some setup
    with cur.execute( '''
    DROP TABLE IF EXISTS DailyTrades;
    DROP TABLE IF EXISTS Brokerages;
    DROP TABLE IF EXISTS Stocks;
    DROP TABLE IF EXISTS Dates;
    '''
    ):
        print( 'Successfuly Del all Table' )
        
    DROP TABLE IF EXISTS Brokerages;
    DROP TABLE IF EXISTS Stocks;
    DROP TABLE IF EXISTS Dates;

def CreateDailyTrade( cur ):

    with cur.execute( '''
    CREATE TABLE dbo.DailyTrades
	(
        id int NOT NULL IDENTITY (1, 1),
        brokerage_id int NOT NULL,
        stock_id int NOT NULL,
        date_id int NOT NULL,
        price real NOT NULL,
        buy_volume int NOT NULL,
        sell_volume int NOT NULL
	)  ON [PRIMARY]
    
    ALTER TABLE dbo.DailyTrades ADD CONSTRAINT
	PK_DailyTrades PRIMARY KEY CLUSTERED 
	(
	    id
	) WITH( STATISTICS_NORECOMPUTE = OFF, 
	IGNORE_DUP_KEY = OFF, 
	ALLOW_ROW_LOCKS = ON, 
	ALLOW_PAGE_LOCKS = ON ) ON [PRIMARY]

    ALTER TABLE dbo.DailyTrades SET (LOCK_ESCALATION = TABLE)
    
    COMMIT
    '''
    ):
        print( 'Successfuly Create Daily Trade')

def CreateStocks( cur ):

    with cur.execute( '''
    CREATE TABLE dbo.Stocks
	(
        id int NOT NULL IDENTITY (1, 1),
        symbol nvarchar(50) NOT NULL,
        name nvarchar(50) NOT NULL
	)  ON [PRIMARY]
	
	ALTER TABLE dbo.Stocks ADD CONSTRAINT
	PK_Stocks PRIMARY KEY CLUSTERED 
	(
	    id
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    CREATE UNIQUE NONCLUSTERED INDEX IX_Stocks ON dbo.Stocks
	(
	name
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    CREATE UNIQUE NONCLUSTERED INDEX IX_Stocks_1 ON dbo.Stocks
	(
	symbol
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    ALTER TABLE dbo.Stocks SET (LOCK_ESCALATION = TABLE)

    COMMIT
    ''' ):
        print( 'Successfuly Create Stocks' )

def CreateDates( cur ):

    with cur.execute( '''
    
    CREATE TABLE dbo.Dates
	(
        id int NOT NULL IDENTITY (1, 1),
        date date NOT NULL
	)  ON [PRIMARY]

    ALTER TABLE dbo.Dates ADD CONSTRAINT
	PK_Dates PRIMARY KEY CLUSTERED 
	(
	    id
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    CREATE UNIQUE NONCLUSTERED INDEX IX_Dates ON dbo.Dates
	(
	    date
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    ALTER TABLE dbo.Dates SET (LOCK_ESCALATION = TABLE)

    COMMIT
    '''):
        print( 'Successfuly Create Dates' )

def CreateBrokerages( cur ):

    with cur.execute( '''
    CREATE TABLE dbo.Brokerages
	(
        id int NOT NULL IDENTITY (1, 1),
        symbol nvarchar(50) NOT NULL,
        name nvarchar(50) NOT NULL
	)  ON [PRIMARY]

    ALTER TABLE dbo.Brokerages ADD CONSTRAINT
	PK_Brokerages PRIMARY KEY CLUSTERED 
	(
	    id
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    CREATE UNIQUE NONCLUSTERED INDEX IX_Brokerages ON dbo.Brokerages
	(
	    name
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    CREATE UNIQUE NONCLUSTERED INDEX IX_Brokerages_1 ON dbo.Brokerages
	(
        symbol
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    ALTER TABLE dbo.Brokerages SET (LOCK_ESCALATION = TABLE)

    COMMIT'''):
        print( 'Successfuly Create Brokerages' )

class dbHandle( ):

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
            return cur.execute( 'SELECT id FROM ' + tablename + ' WHERE ' + fieldname + ' = (?)', (value,) ).fetchone( )[ 0 ]
        else:
            return ft[ 0 ]

    def insert_csv2DB( self, filename ):

        fs = filename.split( '_' )

        # try:
        date = fs[ 1 ].split( '/' )[ 0 ]
        date = date[:4] + '-' + date[ 4:6 ] + '-' + date[ 6: ]
        stock_symbol = re.sub( '.+/', '', filename )[ 0:4 ]
        stock_name = re.sub( '[0-9]+/[0-9]+', '', filename.split( '_' )[ 1 ] )

        f = open( filename, 'r', encoding = 'utf8', errors = 'ignore' )

        for row in csv.reader( f ):
            if row[ 0 ] == '':
                return True

            brokerage_symbol = row[ 1 ][ 0:4 ]
            brokerage_name = row[ 1 ][ 4:len( row[ 1 ] ) ].replace( ' ', '' ).replace( '\u3000', '' )
            price = row[ 2 ]

            buy_volume = row[ 3 ]
            sell_volume = row[ 4 ]

            # brokerage
            ft = self.cur_db.execute( 'SELECT TOP ( 1 ) id FROM Brokerages WHERE symbol = ? ', ( brokerage_symbol, ) ).fetchone( )

            if ft is None:
                self.cur_db.execute( 'INSERT INTO Brokerages (symbol, name) VALUES ( ?, ?) ', brokerage_symbol, brokerage_name )

                self.cur_db.execute( 'SELECT TOP 1 id FROM Brokerages WHERE symbol = ? AND name = ? ', brokerage_symbol, brokerage_name )

                brokerage_id = self.cur_db.fetchone( )[ 0 ]
            else:
                brokerage_id = ft[ 0 ]

            # stock
            ft = self.cur_db.execute( 'SELECT TOP 1 id FROM Stocks WHERE symbol = ?', (stock_symbol,) ).fetchone( )

            if ft is None:
                self.cur_db.execute( 'INSERT INTO Stocks (symbol, name) VALUES ( ?, ? )',
                                     (stock_symbol, stock_name) )
                self.cur_db.execute( 'SELECT TOP 1 id FROM Stocks WHERE symbol = ? AND name = ?',
                                     (stock_symbol, stock_name) )
                stock_id = self.cur_db.fetchone( )[ 0 ]
            else:
                stock_id = ft[ 0 ]

            # date
            date_id = self._insertGetID( self.cur_db, 'Dates', 'date', date )

            # DailyTrades
            self.cur_db.execute(
                'INSERT INTO DailyTrades (brokerage_id, stock_id, date_id, price, buy_volume, sell_volume ) VALUES ( ?, ?, ?, ?, ?, ? )',
                ( brokerage_id, stock_id, date_id, price, buy_volume, sell_volume ) )

    def insertDB( self ):

        print( os.listdir( '.\\' ) )

        for d in os.listdir( '.\\' ):

            if len( d.split( '_' ) ) != 2:
                continue

            date = d.split( '_' )[ 1 ]

            if date.isdigit( ):

                print( 'Processing Date... ' + str( date ) )

                filenames = os.listdir( '.\\' + d )

                print( 'csv file',  len( filenames ), filenames[ -1 ] )

                for fname in filenames:
                    if fname.endswith( ".csv" ):
                        self.insert_csv2DB( './' + d + '/' + fname )

                self.con_db.commit( )


def main( ):
    start = datetime.now( )
    server   = 'localhost'
    database = 'StockDB'
    username = 'sa'
    password = 'admin'

    db = dbHandle( server, database, username, password )

    resetTable( db.cur_db ) # 這句會刪掉SQL內所有資料
    CreateDailyTrade( db.cur_db )
    CreateStocks( db.cur_db )
    CreateDates( db.cur_db )
    CreateBrokerages( db.cur_db )

    db.insertDB( )
    print( datetime.now( ) - start )

if __name__ == '__main__':
    main( )