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

def foo( in_df ):

    for i in range( len( Start ) ):

        # start_date  = start_date_obj.strftime( '%Y%m%d' )
        # end_date    = end_date_obj.strftime( '%Y%m%d' )

        start_date  = Start[ i ].strftime( '%Y%m%d' )
        end_date    = End[ i ].strftime( '%Y%m%d' )

        mask = ( in_df[ '日期' ] >= start_date ) & ( in_df[ '日期'] <= end_date )

        df = in_df.loc[ mask ]

        if df[ '買進股數' ].sum( ) is 0:
            in_df[start_date + '買進均價'] = 0
        else:
            # -------------------------------------------------------
            # 近幾日買進均價
            # -------------------------------------------------------
            in_df[ start_date + '買進均價' ] = df[ '買進價格*股數' ].sum( ) / df[ '買進股數' ].sum( )

        if  df[ '賣出股數' ].sum( ) is 0:
            in_df[start_date + '賣出均價'] = 0
        else:
            # -------------------------------------------------------
            # 近幾日賣出均價
            # -------------------------------------------------------
            in_df[ start_date + '賣出均價' ] = df[ '賣出價格*股數' ].sum( ) / df[ '賣出股數' ].sum( )

        # -------------------------------------------------------
        # 近幾日買賣超
        # -------------------------------------------------------
        in_df[ start_date + '買賣超' ] = df[ '買賣超' ].sum( )
        # -------------------------------------------------------

        # 近幾日買賣超金額
        # -------------------------------------------------------
        in_df[ start_date + '買賣超金額' ] = df[ '買進價格*股數' ].sum( ) - df[ '賣出價格*股數' ].sum( )

    del in_df[ '買進股數' ]
    del in_df[ '賣出股數' ]
    del in_df[ '買進價格*股數' ]
    del in_df[ '賣出價格*股數' ]
    del in_df[ '買賣超' ]
    del in_df[ '買進均價' ]
    del in_df[ '賣出均價' ]
    del in_df[ '日期' ]

    in_df[ '券商' ] = in_df[ '券商' ].apply( remove_whitespace )
    in_df.drop_duplicates( subset='券商', inplace='True' )

    return in_df


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
    #print( "input_df[ '買賣超' ]", input_df[ '買賣超' ] )
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


def Chip_AddDate( input_df, date ):

    # --------------------------------------------------------
    # Function Name : Chip_OneDay_Sort
    # Description   : 刪除欄位-序號，新增欄位-日期
    # Input         : data frame, str
    # Output        : data frame
    # --------------------------------------------------------

    # -------------------------------------------------------
    # 刪除序號欄位，axis = 1 代表x軸
    # 刪除價格欄位，axis = 1 代表x軸
    # -------------------------------------------------------

    # del input_df[ '價格' ]
    # del input_df[ '序號' ]
    # input_df.drop( '序號', axis=1, level=None, inplace=True )
    # input_df.drop( '價格', axis=1, level=None, inplace=True )
    # -------------------------------------------------------

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

    # print( "start_date_list", start_date_list )
    # print( "end_date_list", end_date_list )
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
df_freq_buy  = pd.Series( )
df_freq_self = pd.Series( )

# InputPath        = "..\\籌碼資料\\"
# input_chip_str   = "1218泰山"
# tar_str          = "1218泰山籌碼整理"
# start_date       = "20161214"
# end_date         = "20161230"
# cycle_chip       = 3
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

df_sort = Chip_OneDay_Sort( df_sort )

grouped = df_sort.groupby( [ '券商', '日期'], sort=False )

df_sort.sort_values( [ '券商', '日期' ], inplace=True )

df_sort[ '日期' ] = pd.to_datetime( df_sort[ '日期' ], format='%Y%m%d' )

df_sort = df_sort[ df_sort[ '券商' ].notnull( ) ]

grouped = df_sort.groupby( '券商', sort=False )

df_sort = grouped.apply( foo )
# ------------------------------------------------------------------------------

# 取出日期範圍內買賣超金額前20大，保留買賣超金額，卷商，買進均價
# 日期範圍從Start, End時間物件得到
# 產生dataframe表格數量從Start, End時間物件得出
#--------------------------------------------------------------------------------

for i in range( len( Start ) ):

    start_date  = Start[ i ].strftime( '%Y%m%d' )
    end_date    = End[ i ].strftime( '%Y%m%d' )

    df_sort.reset_index( 0, drop=True, inplace = True )
    # ------------------------------------------------------------------------------

    # 取出含有字串買賣超金額及券商的columns為另一個Dataframe
    # ------------------------------------------------------------------------------

    df_buy20 = df_sort[  df_sort[ start_date + '買賣超金額' ] > 0 ].copy( )

    chip_buy_count = df_buy20[ start_date + '買賣超' ].count()

    del df_buy20[ start_date + '賣出均價' ]

    df_buy20.sort_values( by = start_date + '買賣超金額', axis = 0, inplace = True, ascending = False )

    df_buy20 = df_buy20[ :20 ]

    # print( '買超金額 > 0 ')
    # print( df_buy20 )
    #-------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------
    # 取出含有字串買賣超金額及券商的columns為另一個Dataframe
    # ------------------------------------------------------------------------------

    df_self20 = df_sort[  df_sort[ start_date + '買賣超金額' ] < 0 ].copy( )

    chip_self_count = df_self20[start_date + '買賣超'].count()

    del df_self20[ start_date + '買進均價' ]

    df_self20.sort_values( by = start_date + '買賣超金額', axis = 0, inplace = True, ascending = True )

    df_self20 = df_self20[ :20 ]

    # print( '賣超金額 > 0 ')
    # print( df_self20 )
    #-------------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    # 計算前20大買進券商出現次數
    # 計算前20大賣出券商出現次數
    # ------------------------------------------------------------------------------
    df_freq_buy = df_freq_buy.append( df_buy20[ '券商' ] )
    df_freq_self = df_freq_self.append( df_self20[ '券商' ] )
    # ----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    #計算前20大買進均價
    #計算前20大賣出均價
    #計算前20大買超佔股本比
    #計算前20大賣超佔股本比
    #------------------------------------------------------------------------------

    df_tmp = pd.DataFrame(

        { '日期範圍' : start_date,

        '前20大買進均價' : df_buy20[ start_date + '買賣超金額' ].sum( ) / df_buy20[ start_date + '買賣超' ].sum( ),

        '前20大買超佔股本比' : df_buy20[ start_date + '買賣超金額' ].sum( ) / CapitalStock * 100,

        '前20大賣出均價': df_self20[ start_date + '買賣超金額' ].sum( ) / df_self20[ start_date + '買賣超' ].sum( ),

        '前20大賣超佔股本比' : df_self20[ start_date + '買賣超金額' ].sum( ) / CapitalStock * 100,

        '卷商買家數' : chip_buy_count,

        '卷商賣家數' : chip_self_count,

        '卷商總買賣家數' : chip_buy_count + chip_self_count

        }, index=[ 0 ] )

    print( " 型態 {} ".format( type( df_sort[ start_date + '買賣超金額' ] ) ) )

    df_cal = pd.concat( [ df_cal, df_tmp ] )

    #-----------------------------------------------------------------------------

# df_cal[ '累積前20大買超佔股本比' ] = df_cal[ '前20大買超佔股本比' ].cumsum( )
# df_cal[ '累積前20大賣超佔股本比' ] = df_cal[ '前20大賣超佔股本比' ].cumsum( )
#整組DataFrame根據index翻轉排序

df_cal = df_cal.iloc[ ::-1 ]

#df_cal[ '累積前20大買超佔股本比' ] -= df_cal.loc[ -1, '累積前20大買超佔股本比' ]
#df_cal[ '累積前20大賣超佔股本比' ] -= df_cal.loc[ -1, '累積前20大賣超佔股本比' ]
#---------------------------------------------------------------------------------

df_freq_buy  = df_freq_buy.value_counts( )[ :20 ]
df_freq_self = df_freq_self.value_counts( )[ :20 ]

print( df_freq_buy )
print( df_freq_self )

df_writer = pd.ExcelWriter( tar_str + '.xlsx'  )
df_sort.to_excel( df_writer, sheet_name = '籌碼分析' )
#---------------------------------------------------

# 排序寫入欄位
# 日期範圍
# 前20大買超佔股本比
# 前20大賣超佔股本比
# 前20大買進均價
# 前20大賣出均價
#-------------------------------------------------

# df_cal[ '當日收盤價' ] =

cols = [ '日期範圍', '前20大買超佔股本比', '前20大賣超佔股本比',
         '前20大買進均價', '前20大賣出均價', '卷商總買賣家數', '卷商買家數', '卷商賣家數' ]

df_cal = df_cal.reindex( columns = cols )

# ------------------------------------------------
# 取得收盤價
# ------------------------------------------------
ret = GetClosePrice( input_chip_str, global_start_date )

df_cal = pd.merge( df_cal, ret, how = 'inner', on = '日期範圍' )

C = np.array( df_cal[ 'Close' ], dtype=float, ndmin=1 )
H = np.array( df_cal[ 'High' ],  dtype=float, ndmin=1 )
L = np.array( df_cal[ 'Low' ],   dtype=float, ndmin=1 )
V = np.array( df_cal[ 'Volume' ],dtype=float, ndmin=1 )

df_cal['MA03']   = talib.SMA( C, 3 )
df_cal['MA05']   = talib.SMA( C, 5 )
df_cal['MA10']   = talib.SMA( C, 10 )
df_cal['MA20']   = talib.SMA( C, 20 )
df_cal['MA30']   = talib.SMA( C, 30 )
df_cal['MA45']   = talib.SMA( C, 45 )
df_cal['MA60']   = talib.SMA( C, 60 )
df_cal['MA120']  = talib.SMA( C, 120 )

df_cal['RSI 12'] = talib.RSI( C, timeperiod=12 )

# ------ MACD Begin. ----------------------------
# 使用MACD需要设置长短均线和macd平均线的参数
SHORTPERIOD  = 12
LONGPERIOD   = 26
SMOOTHPERIOD = 9
# 用Talib计算MACD取值，得到三个时间序列数组，分别为macd,signal 和 hist
DIF = ( H + L +  2 * C  ) / 4
df_cal['MACD DIF'], df_cal['DEM'], df_cal['OSC'] = talib.MACD( DIF, SHORTPERIOD, LONGPERIOD, SMOOTHPERIOD )
# ------ MACD End. ------------------------------

# -------- MFI Begin. ---------------------------
df_cal[ 'MFI(6)' ]  = talib.MFI( H, L, C, V, timeperiod = 6 )
df_cal[ 'MFI(14)' ] = talib.MFI( H, L, C, V, timeperiod = 14 )
# -------- MFI End. -----------------------------

# -------- Williams %R Begin. ------------------------
df_cal[ 'WILLR 9' ]  = talib.WILLR( H, L, C, timeperiod=9 )
df_cal[ 'WILLR 18' ] = talib.WILLR( H, L, C, timeperiod=18 )
df_cal[ 'WILLR 56' ] = talib.WILLR( H, L, C, timeperiod=56 )
# -------- Williams %R End. --------------------------

# -------- Average Directional Movement Index Begin . --------
df_cal[ 'PLUS_DI']  = talib.PLUS_DI( H, L, C, timeperiod = 14 )
df_cal[ 'MINUS_DI'] = talib.MINUS_DI( H, L, C, timeperiod = 14 )
df_cal[ 'DX']       = talib.DX( H, L, C, timeperiod = 14 )
df_cal[ 'ADX']      = talib.ADX( H, L, C, timeperiod = 14 )
# ------- Average Directional Movement Index End . --------

# -------- Bollinger Bands Begin. --------
# 布林 是 OK，但倒過來
df_cal[ 'Upperband' ], df_cal[ 'Middleband' ], df_cal[ 'Dnperband' ] = talib.BBANDS( C, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0 )
df_cal['%BB'] = ( C - df_cal[ 'Dnperband' ] ) / ( df_cal[ 'Upperband' ] - df_cal[ 'Dnperband' ] )
df_cal['W20'] = ( df_cal[ 'Upperband' ] - df_cal[ 'Dnperband' ] ) / df_cal[ 'MA20' ]
# -------- Bollinger Bands Begin. --------

# ---------------- 乖離 指標 Begin. ------------------------
# 乖離 OK, 但比較是倒過來
# 20 Bias=(C-SMA20)/SMA20
# 60 Bias=(C-SMA60)/SMA60
df_cal[ '20 Bias' ] = ( C - df_cal['MA20'] ) / df_cal['MA20']
df_cal[ '60 Bias' ] = ( C - df_cal['MA60'] ) / df_cal['MA60']
# ---------------- 乖離 指標 End. ------------------------

df_cal.to_excel( df_writer, sheet_name = '買賣超金額20大' )

tmp_1 = df_freq_buy.to_frame( )

tmp_1.columns  = [ '累加買進次數' ]

tmp_2 = df_freq_self.to_frame( )

tmp_2.columns  = [ '累加賣出次數' ]

result = pd.concat( [ tmp_1, tmp_2 ], axis=1 )

result.to_excel( df_writer, sheet_name = '買賣超券商' )

df_writer.save( )

print( '完成' )

# df[ '當日交易卷商數' ]
# df[ '當日交易卷商買家數' ]
# df[ '當日交易卷商賣家數' ]
# df[ 'MA' ]
# df[ 'KD' ]
# df[ 'RSI' ]
# df[ 'MACD' ]
# df[ 'W%R' ]
# df[ 'BB Band' ]
# df[ 'DMI' ]
# df[ 'MFI' ]
# df[ 'BIAD' ]
#------------------------------------------------------------

# df[ '單日前20大買超# 金額' ]
# df[ '單日前20大買超張數' ]
# df[ '單日前20大買進均價' ]
# df[ '單日前20大買超金額佔股本比' ]
#------------------------------------------------------------

# df[ '單日前20大賣超金額' ]
# df[ '單日前20大賣超張數' ]


