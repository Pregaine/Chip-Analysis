# -*- coding: utf-8 -*-
import pandas as pd
import os
import re
import glob
import sys

def foo( in_df ):

    for date in start_date:

        mask = ( in_df[ '日期' ] >= date ) & ( in_df[ '日期'] <= end_date )

        df = in_df.loc[ mask ]

        try:
            # -------------------------------------------------------
            # 近幾日買進均價
            # -------------------------------------------------------

            in_df[ date + '~' + end_date + '買進均價' ] = df[ '買進價格*股數' ].sum( ) / df[ '買進股數' ].sum( )
        except:
            in_df[ date + '~' + end_date + '買進均價' ] = 0
            pass

        try:
            # -------------------------------------------------------
            # 近幾日賣出均價
            # -------------------------------------------------------

            in_df[ date + '~' + end_date + '賣出均價' ] = df[ '賣出價格*股數' ].sum( ) / df[ '賣出股數' ].sum( )
        except:
            in_df[ date + '~' + end_date + '賣出均價' ] = 0
            pass

        # -------------------------------------------------------
        # 近幾日買賣超
        # -------------------------------------------------------

        in_df[ date + '~' + end_date + '買賣超' ] = df[ '買賣超' ].sum( )

        # -------------------------------------------------------
        # 近幾日買賣超金額
        # -------------------------------------------------------

        in_df[ date + '~' + end_date + '買賣超金額' ] = df[ '買進價格*股數' ].sum( ) - df[ '賣出價格*股數' ].sum( )

    return in_df
#--------------------------------------------------------
#Function Name : Chip_OneDay_Sort
#Description   : 整理單日數據
#Input         : data frame, str
#Output        : data frame
#--------------------------------------------------------

def Chip_OneDay_Sort( input_df ):

    #--------------------------------------------------------
    #新增欄位-買進價格*股數
    #新增欄位-賣出價格*股數
    #--------------------------------------------------------

    input_df[ '買進價格*股數' ] = input_df[ '買進股數' ] * input_df[ '價格' ]
    input_df[ '賣出價格*股數' ] = input_df[ '賣出股數' ] * input_df[ '價格' ]
    #--------------------------------------------------------

    # --------------------------------------------------------
    # 依據券商群組分類
    # --------------------------------------------------------

    # input_df = input_df.groupby( '券商' ).sum( )
    # --------------------------------------------------------

    # --------------------------------------------------------
    # 新增欄位-買賣超
    # 新增欄位-買進均價
    # 新增欄位-賣出均價
    # --------------------------------------------------------

    input_df[ '買賣超' ] = input_df[ '買進股數' ] - input_df[ '賣出股數' ]
    input_df[ '買進均價' ] = input_df[ '買進價格*股數' ] / input_df[ '買進股數' ]
    input_df[ '賣出均價' ] = input_df[ '賣出價格*股數' ] / input_df[ '賣出股數' ]
    # --------------------------------------------------------

    # -------------------------------------------------------
    # 刪除序號欄位，axis = 1 代表x軸
    # 刪除價格欄位，axis = 1 代表x軸
    # -------------------------------------------------------

    input_df.drop( '價格', axis=1, level=None, inplace=True )
    input_df.drop( '序號', axis=1, level=None, inplace=True )
    # -------------------------------------------------------

    return input_df

#--------------------------------------------------------
#Function Name : Chip_OneDay_Sort
#Description   : 刪除欄位-序號，新增欄位-日期
#Input         : data frame, str
#Output        : data frame
#--------------------------------------------------------

def Chip_AddDate( input_df, date ):

    # -------------------------------------------------------
    # 刪除序號欄位，axis = 1 代表x軸
    # 刪除價格欄位，axis = 1 代表x軸
    # -------------------------------------------------------

    # del input_df[ '價格' ]
    # del input_df[ '序號' ]
    # input_df.drop( '?序號', axis=1, level=None, inplace=True )
    # input_df.drop( '價格', axis=1, level=None, inplace=True )
    # -------------------------------------------------------

    # --------------------------------------------------------
    # 新增欄位-日期
    # --------------------------------------------------------
    input_df['日期'] = date

    return input_df

df_sort = pd.DataFrame( )
# InputPath   = 'D:\\03-workspace\\02-卷商分點\\搜集籌碼資料\\'
# start_date  = '2017-01-13'
# mid1_date   = '2017-01-15'
# mid2_date   = '2017-01-18'
# end_date    = '2017-01-19'
# chip_str    = '1723中碳*.csv'
# tar_str     = '1723中碳籌碼整理'

InputPath        =  sys.argv[ 1 ]
input_chip_str   =  sys.argv[ 2 ]
tar_str          =  sys.argv[ 3 ]
end_date         =  sys.argv[ 4 ]
start_date       =  sys.argv[ 5 ].split( ',' )

print( start_date, end_date )

chip_str    = input_chip_str + '*.csv'

for input_file in glob.glob( os.path.join( InputPath, chip_str ) ):

    re_obj = re.compile( r'[0-9]{8}' )
    YearPath = re_obj.search( input_file )

    print( 'Year', YearPath.group( 0 ) )

    # --------------------------------------------------------
    # 讀取CSV File至DataFrame
    # quotechar 代表包住字串符號
    # delimiter 代表分隔符號
    # --------------------------------------------------------

    filename_str = input_file
    print( filename_str )
    df = pd.read_csv( filename_str, sep  = ',', encoding  = 'cp950', false_values = 'NA', names = [ '序號', '券商', '價格', '買進股數', '賣出股數' ] )
    # --------------------------------------------------------

    df = Chip_AddDate( df, YearPath.group( 0 ) )
    df_sort = pd.concat( [ df_sort, df ] )

df_sort = Chip_OneDay_Sort( df_sort )

grouped = df_sort.groupby( [ '券商', '日期'], sort=False )

df_sort.sort_values( [ '券商', '日期' ], inplace=True )

df_sort[ '日期' ] = pd.to_datetime( df_sort[ '日期' ], format='%Y%m%d' )

df_sort = df_sort[ df_sort[ '券商' ].notnull( ) ]

grouped = df_sort.groupby( '券商', sort=False )

# print( df_sort.columns.values )

df_sort = grouped.apply( foo )

# df_sort.to_csv( tar_str + '.csv', sep=',', line_terminator='\n' )

df_writer = pd.ExcelWriter( tar_str + '.xlsx'  )

df_sort.to_excel( df_writer, sheet_name = '籌碼分析' )

df_writer.save( )

print( '完成' )