# -*- coding: utf-8 -*-

import os
import re
import glob
import sys
import datetime
import talib
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup as BS
import sqlite3
import os.path

# Todo
# format 日期格式
# in_df[ start_date + '買進均價' ] = df[ '買進價格*股數' ].sum( ) / df[ '買進股數' ].sum( )
# 解決 invalid value in double scale

def GetClosePrice( input_chip_str, start_date ):

    date    = [ ]
    close   = [ ]
    high    = [ ]
    low     = [ ]
    volume  = [ ]
    price   = 0
    index   = 0
    ret_df = pd.DataFrame( )

    day_obj = datetime.datetime.strptime( start_date, '%Y%m%d' )
    day_str = day_obj.strftime( '%Y/%m/%d' )

    input_chip_str = input_chip_str.strip( )

    match = re.match( r"([0-9]+)(.*)", input_chip_str, re.I )
    if match:
        input_chip_str = match.groups( )

    print( "字串 {} ".format( input_chip_str[ 0 ] ) )
    print( "字串 {} ".format( day_str ) )

    website = 'http://www.cnyes.com/twstock/ps_historyprice.aspx?code=' + input_chip_str[ 0 ] + \
              '&ctl00$ContentPlaceHolder1$startText=' + day_str

    print( "字串 {} ".format( website ) )

    headers = { 'User-Agent': 'Mozilla/5.0' }
    rs = requests.session( )
    res = rs.get( website, stream=True, verify=False, headers=headers )
    soup = BS( res.text, "html.parser" )

    for row in soup.find_all( 'td' )[ 3:-4 ]:

        if index is 0:
            date.append( row.text.replace( "/", "" ) )
        elif index is 4:
            price = row.text.replace( ",", "" )
            price = float( price )
            close.append( price )
        elif index is 2:
            price = row.text.replace( ",", "" )
            price = float( price )
            high.append( price )
        elif index is 3:
            price = row.text.replace( ",", "" )
            price = float( price )
            low.append( price )
        elif index is 7:
            price = row.text.replace( ",", "" )
            price = float( price )
            volume.append( price )

        if index is 9:
            index = 0
        else:
            index = index + 1

    # Create a variable of the value of the columns
    columns = { '日期範圍': date, 'Close': close, 'High': high, 'Low': low, 'Volume': volume }

    # Create a dataframe from the columns variable
    ret_df = pd.DataFrame( columns )

    return ret_df

def Chip_OneDay_Sort( input_df ):

    # --------------------------------------------------------
    # Function Name : Chip_OneDay_Sort
    # Description   : 整理單日數據
    # Input         : data frame, str
    # Output        : data frame
    # --------------------------------------------------------

    #--------------------------------------------------------
    #新增欄位-買進價格*股數
    #新增欄位-賣出價格*股數
    #--------------------------------------------------------
    input_df.loc[ :, '買進價格*股數' ] = input_df[ '買進股數' ] * input_df[ '價格' ]
    input_df.loc[ :, '賣出價格*股數' ] = input_df[ '賣出股數' ] * input_df[ '價格' ]
    #--------------------------------------------------------

    # --------------------------------------------------------
    # 新增欄位-買賣超
    # 新增欄位-買進均價
    # 新增欄位-賣出均價
    # --------------------------------------------------------
    input_df.loc[ :, '買賣超' ] = input_df[ '買進股數' ] - input_df[ '賣出股數' ]
    input_df.loc[ :, '買進均價' ] = input_df[ '買進價格*股數' ] / input_df[ '買進股數' ]
    input_df.loc[ :, '賣出均價' ] = input_df[ '賣出價格*股數' ] / input_df[ '賣出股數' ]
    # --------------------------------------------------------

    # -------------------------------------------------------
    # 刪除序號欄位，axis = 1 代表x軸
    # 刪除價格欄位，axis = 1 代表x軸
    # -------------------------------------------------------
    input_df.drop( '價格', axis=1, level=None, inplace=True )
    input_df.drop( '序號', axis=1, level=None, inplace=True )
    # -------------------------------------------------------

    return input_df


def Chip_AddDate( input_df, date ):

    # --------------------------------------------------------
    # 新增欄位-日期
    # --------------------------------------------------------
    input_df['日期'] = date

    return input_df

def Cal_ChipDateList( Path, chip_str, start_date_str, end_date_str, cycle ):

    start_date_list = [ ]
    end_date_list   = [ ]
    file_list       = [ ]
    time_list       = [ ]

    theday_obj = datetime.datetime.strptime( start_date_str, '%Y%m%d' )
    endday_obj = datetime.datetime.strptime( end_date_str, '%Y%m%d' )
    theday_str = theday_obj.strftime( '_%Y%m%d' )
    # ----------------------------------------------------

    while theday_obj <= endday_obj:

        FilePath = Path + '全台卷商交易資料' + theday_str + '\\' + chip_str + theday_str + '.csv'

        if os.path.exists( FilePath ):
            file_list.append( FilePath )
            time_list.append( theday_obj )

        theday_obj += datetime.timedelta( days=1 )
        theday_str = theday_obj.strftime( '_%Y%m%d' )

    time_obj = [ time_list[ i : i + cycle ]  for i in range( 0, len( time_list ), cycle ) ]

    for i in time_obj:
        start_date_list.append( i[ 0 ] )
        end_date_list.append( i[ - 1 ] )

    start_date_list = start_date_list
    end_date_list = end_date_list

    return start_date_list, end_date_list, file_list


def remove_whitespace(x):
    try:
        # Remove spaces inside of the string
        x = "".join( x.split( ) )
    except:
        pass
    return x

df_sort      = pd.DataFrame( )
df_cal       = pd.DataFrame( )
df_compare   = pd.DataFrame( )
df_freq_buy  = pd.Series( )
df_freq_self = pd.Series( )

# InputPath        = "..\\籌碼資料\\"
# input_chip_str   = "1723中碳"
# tar_str          = "1723中碳籌碼整理"
# start_date       = "20170407"
# end_date         = "20170414"
# cycle_chip       = 1
# CapitalStock     = 3530000000

InputPath        =  sys.argv[ 1 ]
input_chip_str   =  sys.argv[ 2 ]
tar_str          =  sys.argv[ 3 ]
start_date       =  sys.argv[ 4 ]
end_date         =  sys.argv[ 5 ]
cycle_chip       =  sys.argv[ 6 ]
CapitalStock     =  sys.argv[ 7 ]

cycle_chip       = int( cycle_chip )
CapitalStock     = int( CapitalStock )

global_start_date = start_date

print( "股本", CapitalStock )

Start, End, File = Cal_ChipDateList( InputPath, input_chip_str, start_date, end_date, cycle_chip )

# print( 'Start List', Start )
# print( 'End List', End )
# print( File, len( File ) )
# ---------------------------------------------------
# 時間字串格式判定
# ---------------------------------------------------
# start_time_obj = datetime.strptime( start_date[ 0 ], '%Y%m%d' )
# end_time_obj   = datetime.strptime( end_date, '%Y%m%d' )
# ---------------------------------------------------

# chip_str    = input_chip_str + '*.csv'

# if os.path.isfile( tar_str + '.sqlite' ):
#
#     with sqlite3.connect( tar_str + '.sqlite' ) as db:
#
#         df_compare = pd.read_sql_query( 'select * from df', con = db )
#         # 讀出 df_comapre 日期範圍
#         print( df_compare[ '日期範圍' ] )
#         print( Start )
#
#         for val in df_compare[ '日期範圍' ]:
#
#             date_obj = datetime.datetime.strptime( val, '%Y%m%d' )
#
#             if date_obj in Start:
#                 Start.remove( date_obj )
#
#         print( 'Start', Start )
#
# if len( Start ) == 0:
#     print( 'exit' )
#     exit()

for input_file in File:

    re_obj = re.compile( r'[0-9]{8}' )
    YearPath = re_obj.search( input_file )

    # --------------------------------------------------------
    # 讀取CSV File至DataFrame
    # quotechar 代表包住字串符號
    # delimiter 代表分隔符號
    # --------------------------------------------------------
    filename_str = input_file
    print( filename_str )
    df = pd.read_csv( filename_str, sep  = ',', encoding  = 'cp950', false_values = 'NA', names = [ '序號', '券商', '價格', '買進股數', '賣出股數' ] )
    # --------------------------------------------------------

    df      = Chip_AddDate( df, YearPath.group( 0 ) )
    df_sort = pd.concat( [ df_sort, df ] )

df_sort = df_sort[ df_sort[ '券商' ].notnull( ) ]

df_sort = Chip_OneDay_Sort( df_sort )

df_sort[ '日期' ] = pd.to_datetime( df_sort[ '日期' ], format='%Y%m%d' )

# df_sort.set_index( [ '券商', '日期' ], inplace=True )

# df_sort.reset_index( inplace=True )

df_sort.groupby( [ '券商', '日期' ] ).sum().reset_index( 0, drop=True, inplace = True )

df_sort[ '買進均價' ] = df_sort[ '買進價格*股數' ] / df_sort['買進股數']
df_sort[ '賣出均價' ] = df_sort[ '賣出價格*股數' ] / df_sort['賣出股數']
# ------------------------------------------------------------------------------

# 取出日期範圍內買賣超金額前15大，保留買賣超金額，卷商，買進均價
# 日期範圍從Start, End時間物件得到
# 產生dataframe表格數量從Start, End時間物件得出
#--------------------------------------------------------------------------------

for i in range( len( Start ) ):

    start_date  = Start[ i ].strftime( '%Y%m%d' )
    # ------------------------------------------------------------------------------

    # 取出含有字串買賣超金額及券商的columns為另一個Dataframe
    # ------------------------------------------------------------------------------

    df_buy15 = df_sort[ ( df_sort[ '買進均價' ] > 0 ) & ( df_sort[ '日期' ] == Start[ i ] ) ].copy( )

    chip_buy_count = df_buy15.drop_duplicates(subset=['券商', '日期'], keep='first')['券商'].count()

    print( '買家數量',Start[ i ], chip_buy_count )

    df_buy15['買賣超金額'] = df_buy15['買進價格*股數'] - df_buy15['賣出價格*股數']

    df_buy15.sort_values(by='買賣超金額', axis=0, ascending=False, inplace = True )

    df_buy15 = df_buy15[ :15 ]
    #-------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------
    # 取出含有字串買賣超金額及券商的columns為另一個Dataframe
    # ------------------------------------------------------------------------------

    df_self15 = df_sort[ ( df_sort[ '賣出均價' ] > 0 ) & ( df_sort[ '日期' ] == Start[ i ] ) ].copy( )

    chip_self_count = df_self15.drop_duplicates(subset=['券商', '日期'], keep='first')['券商'].count()

    print('賣家數量', Start[i], chip_buy_count)

    df_self15[ '買賣超金額'] = df_self15['買進價格*股數'] - df_self15['賣出價格*股數']

    df_self15 = df_self15.sort_values( by='買賣超金額', axis=0, ascending=True )

    df_self15 = df_self15[ :15 ]
    # ------------------------------------------------------------------------------

    list_all_chip = df_sort[ ( (df_sort['買進均價'] > 0) | (df_sort['賣出均價'] > 0) ) & (df_sort['日期'] == Start[i] ) ][ '券商' ]

    print( 'list_all_chip', list_all_chip )

    list_all_chip_count = len( set( list_all_chip ) )
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    # 計算前15大買進券商出現次數
    # 計算前15大賣出券商出現次數
    # ------------------------------------------------------------------------------
    df_freq_buy = df_freq_buy.append( df_buy15[ '券商' ] )
    df_freq_self = df_freq_self.append( df_self15[ '券商' ] )
    # ----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    #計算前15大買進均價
    #計算前15大賣出均價
    #計算前15大買超佔股本比
    #計算前15大賣超佔股本比
    #------------------------------------------------------------------------------

    df_tmp = pd.DataFrame(

        { '日期範圍' : start_date,

        '前15大買進均價' : df_buy15[ '買賣超金額' ].sum( ) / df_buy15[ '買賣超' ].sum( ),

        '前15大買超佔股本比' : df_buy15[ '買賣超金額' ].sum( ) / CapitalStock * 100,

        '前15大賣出均價': df_self15[ '買賣超金額' ].sum( ) / df_self15[ '買賣超' ].sum( ),

        '前15大賣超佔股本比' : df_self15[ '買賣超金額' ].sum( ) / CapitalStock * 100,

        '前15大買賣超張數' : df_buy15[ '買賣超' ].sum( ) - df_self15[ '買賣超' ].sum( ),

        '當日總卷商買家數' : chip_buy_count,

        '當日總卷商賣家數' : chip_self_count,

        '當日總卷商買賣家數' : list_all_chip_count

        }, index=[ 0 ] )

    df_cal = pd.concat( [ df_cal, df_tmp ] )
    #-----------------------------------------------------------------------------

#整組DataFrame根據index翻轉排序

df_cal = df_cal.iloc[ ::-1 ]
#---------------------------------------------------------------------------------

df_freq_buy  = df_freq_buy.value_counts( )[ :15 ]
df_freq_self = df_freq_self.value_counts( )[ :15 ]

df_writer = pd.ExcelWriter( tar_str + '.xls'  )
df_sort.to_excel( df_writer, sheet_name = '籌碼分析' )
#---------------------------------------------------

# 排序寫入欄位
# 日期範圍
# 前15大買超佔股本比
# 前15大賣超佔股本比
# 前15大買進均價
# 前15大賣出均價
#-------------------------------------------------

df_cal.loc[ :, '股本' ] = CapitalStock;

cols = [ '股本', '日期範圍', '前15大買超佔股本比', '前15大賣超佔股本比', '前15大買賣超張數',
         '前15大買進均價', '前15大賣出均價', '當日總卷商買賣家數', '當日總卷商買家數', '當日總卷商賣家數' ]

df_cal = df_cal.reindex( columns = cols )

# ------------------------------------------------
# 取得收盤價
# ------------------------------------------------
ret = GetClosePrice( input_chip_str, global_start_date )

# df_cal = pd.merge( df_cal, ret, how = 'inner', on = '日期範圍' )

C = np.array( ret[ 'Close' ], dtype=float, ndmin=1 )
H = np.array( ret[ 'High' ],  dtype=float, ndmin=1 )
L = np.array( ret[ 'Low' ],   dtype=float, ndmin=1 )
V = np.array( ret[ 'Volume' ],dtype=float, ndmin=1 )

ret['MA03']   = talib.SMA( C, 3 )
ret['MA05']   = talib.SMA( C, 5 )
ret['MA10']   = talib.SMA( C, 10 )
ret['MA20']   = talib.SMA( C, 15 )
ret['MA30']   = talib.SMA( C, 30 )
ret['MA45']   = talib.SMA( C, 45 )
ret['MA60']   = talib.SMA( C, 60 )
ret['MA120']  = talib.SMA( C, 120 )
ret['RSI 12'] = talib.RSI( C, timeperiod=12 )

# ------ MACD Begin. ----------------------------
# 使用MACD需要设置长短均线和macd平均线的参数
SHORTPERIOD  = 12
LONGPERIOD   = 26
SMOOTHPERIOD = 9
# 用Talib计算MACD取值，得到三个时间序列数组，分别为macd,signal 和 hist
DIF = ( H + L +  2 * C  ) / 4
ret['MACD DIF'], ret['DEM'], ret['OSC'] = talib.MACD( DIF, SHORTPERIOD, LONGPERIOD, SMOOTHPERIOD )
# ------ MACD End. ------------------------------

# -------- MFI Begin. ---------------------------
ret[ 'MFI(6)' ]  = talib.MFI( H, L, C, V, timeperiod = 6 )
ret[ 'MFI(14)' ] = talib.MFI( H, L, C, V, timeperiod = 14 )
# -------- MFI End. -----------------------------

# -------- Williams %R Begin. ------------------------
ret[ 'WILLR 9' ]  = talib.WILLR( H, L, C, timeperiod=9 )
ret[ 'WILLR 18' ] = talib.WILLR( H, L, C, timeperiod=18 )
ret[ 'WILLR 56' ] = talib.WILLR( H, L, C, timeperiod=56 )
# -------- Williams %R End. --------------------------

# -------- Average Directional Movement Index Begin . --------
ret[ 'PLUS_DI']  = talib.PLUS_DI( H, L, C, timeperiod = 14 )
ret[ 'MINUS_DI'] = talib.MINUS_DI( H, L, C, timeperiod = 14 )
ret[ 'DX']       = talib.DX( H, L, C, timeperiod = 14 )
ret[ 'ADX']      = talib.ADX( H, L, C, timeperiod = 14 )
# ------- Average Directional Movement Index End . --------

# -------- Bollinger Bands Begin. --------
# 布林 是 OK，但倒過來
ret[ 'Upperband' ], ret[ 'Middleband' ], ret[ 'Dnperband' ] = talib.BBANDS( C, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0 )
ret['%BB'] = ( C - ret[ 'Dnperband' ] ) / ( ret[ 'Upperband' ] - ret[ 'Dnperband' ] )
ret['W20'] = ( ret[ 'Upperband' ] - ret[ 'Dnperband' ] ) / ret[ 'MA20' ]
# -------- Bollinger Bands Begin. --------

# ---------------- 乖離 指標 Begin. ------------------------
# 乖離 OK, 但比較是倒過來
# 20 Bias=(C-SMA20)/SMA20
# 60 Bias=(C-SMA60)/SMA60
ret[ '20 Bias' ] = ( C - ret['MA20'] ) / ret['MA20']
ret[ '60 Bias' ] = ( C - ret['MA60'] ) / ret['MA60']
# ---------------- 乖離 指標 End. ------------------------

df_cal.to_excel( df_writer, sheet_name = '買賣超金額15大' )

ret.to_excel( df_writer, sheet_name='技術指標分析' )

# df_compare.to_excel( df_writer, sheet_name = '比較' )

# with sqlite3.connect( tar_str + '.sqlite' ) as db:
#     df_cal.to_sql( 'df', con = db, if_exists = 'replace' )

# print( df_cal.info( ) )
# print( "----------------------------" )
# print( df_compare.info( ) )

# if df_compare.equals( df_cal ):
#     print( "compare true" )
# else:
#     print( "compare fail" )

tmp_1 = df_freq_buy.to_frame( )

tmp_1.columns  = [ '累加買進次數' ]

tmp_2 = df_freq_self.to_frame( )

tmp_2.columns  = [ '累加賣出次數' ]

result = pd.concat( [ tmp_1, tmp_2 ], axis=1 )

result.to_excel( df_writer, sheet_name = '買賣超券商' )

df_writer.save( )

