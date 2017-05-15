# -*- coding: utf-8 -*-

import datetime
import os
import os.path
import re
import sys
import query_stock_price.query_stock_price_國票證券 as qsp
import pandas as pd


# Todo
# format 日期格式
# in_df[ start_date + '買進均價' ] = df[ '買進價格*股數' ].sum( ) / df[ '買進股數' ].sum( )
# 解決 invalid value in double scale

df_sort = pd.DataFrame( )
df_cal = pd.DataFrame( )
df_compare = pd.DataFrame( )
df_freq_buy = pd.Series( )
df_freq_self = pd.Series( )

# InputPath        = "..\\籌碼資料\\"
# input_chip_str   = "1723中碳"
# tar_str          = "1723中碳籌碼整理"
# start_date       = "20170407"
# end_date         = "20170414"
# cycle_chip       = 1
# CapitalStock     = 3530000000

InputPath = sys.argv[ 1 ]
input_chip_str = sys.argv[ 2 ]
tar_str = sys.argv[ 3 ]
start_date = sys.argv[ 4 ]
end_date = sys.argv[ 5 ]
cycle_chip = sys.argv[ 6 ]
CapitalStock = sys.argv[ 7 ]

cycle_chip = int( cycle_chip )
CapitalStock = int( CapitalStock )

global_start_date = start_date

print( "股本", CapitalStock )

def Cal_ChipDateList( Path, chip_str, start_date_str, end_date_str, cycle ):
    start_date_list = [ ]
    end_date_list = [ ]
    file_list = [ ]
    time_list = [ ]

    theday_obj = datetime.datetime.strptime( start_date_str, '%Y%m%d' )
    endday_obj = datetime.datetime.strptime( end_date_str, '%Y%m%d' )
    theday_str = theday_obj.strftime( '_%Y%m%d' )
    # ----------------------------------------------------

    while theday_obj <= endday_obj:

        FilePath = Path + '全台卷商交易資料' + theday_str + '\\' + chip_str + theday_str + '.csv'

        if os.path.exists( FilePath ):
            file_list.append( FilePath )
            time_list.append( theday_obj )

        theday_obj += datetime.timedelta( days = 1 )
        theday_str = theday_obj.strftime( '_%Y%m%d' )

    time_obj = [ time_list[ i: i + cycle ] for i in range( 0, len( time_list ), cycle ) ]

    for i in time_obj:
        start_date_list.append( i[ 0 ] )
        end_date_list.append( i[ - 1 ] )

    start_date_list = start_date_list
    end_date_list = end_date_list

    return start_date_list, end_date_list, file_list

def remove_whitespace( x ):
    try:
        # Remove spaces inside of the string
        x = "".join( x.split( ) )
    except:
        pass
    return x

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
    df = pd.read_csv( filename_str, sep = ',', encoding = 'cp950', false_values = 'NA',
                      names = [ '序號', '券商', '價格', '買進股數', '賣出股數' ] )
    # --------------------------------------------------------

    df[ '日期' ] = YearPath.group( 0 )
    df_sort = pd.concat( [ df_sort, df ] )

df_sort = df_sort[ df_sort[ '券商' ].notnull( ) ]

df_sort[ '買進價格*股數' ] = df_sort[ '買進股數' ] * df_sort[ '價格' ]
df_sort[ '賣出價格*股數' ] = df_sort[ '賣出股數' ] * df_sort[ '價格' ]
# --------------------------------------------------------

df_sort.drop( '價格', axis = 1, level = None, inplace = True )
df_sort.drop( '序號', axis = 1, level = None, inplace = True )
# -------------------------------------------------------

df_sort[ '買賣超' ]   = df_sort[ '買進股數' ] - df_sort[ '賣出股數' ]
# --------------------------------------------------------

df_sort[ '日期' ] = pd.to_datetime( df_sort[ '日期' ], format = '%Y%m%d' )

df_sort = df_sort.groupby( [ '券商', '日期' ] ).sum( ).reset_index( )

df_sort[ '買進均價' ] = df_sort[ '買進價格*股數' ] / df_sort[ '買進股數' ]
df_sort[ '賣出均價' ] = df_sort[ '賣出價格*股數' ] / df_sort[ '賣出股數' ]

cols = [ '券商', '日期', '買進均價', '賣出均價', '買賣超', '買進價格*股數', '賣出價格*股數' ]

df_sort = df_sort.reindex( columns = cols )
# ------------------------------------------------------------------------------

# 取出日期範圍內買賣超金額前15大，保留買賣超金額，卷商，買進均價
# 日期範圍從Start, End時間物件得到
# 產生dataframe表格數量從Start, End時間物件得出
# --------------------------------------------------------------------------------

for i in range( len( Start ) ):
    start_date = Start[ i ].strftime( '%Y%m%d' )
    # ------------------------------------------------------------------------------

    # 取出含有字串買賣超金額及券商的columns為另一個Dataframe
    # ------------------------------------------------------------------------------

    df_buy15 = df_sort[ (df_sort[ '買進均價' ] > 0) & (df_sort[ '日期' ] == Start[ i ]) ].copy( )

    chip_buy_count = df_buy15.drop_duplicates( subset = [ '券商', '日期' ], keep = 'first' )[ '券商' ].count( )

    df_buy15[ '買賣超金額' ] = df_buy15[ '買進價格*股數' ] - df_buy15[ '賣出價格*股數' ]

    df_buy15.sort_values( by = '買賣超金額', axis = 0, ascending = False, inplace = True )

    df_buy15 = df_buy15[ :15 ]
    # -------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------
    # 取出含有字串買賣超金額及券商的columns為另一個Dataframe
    # ------------------------------------------------------------------------------

    df_self15 = df_sort[ (df_sort[ '賣出均價' ] > 0) & (df_sort[ '日期' ] == Start[ i ]) ].copy( )

    chip_self_count = df_self15.drop_duplicates( subset = [ '券商', '日期' ], keep = 'first' )[ '券商' ].count( )

    df_self15[ '買賣超金額' ] = df_self15[ '買進價格*股數' ] - df_self15[ '賣出價格*股數' ]

    df_self15 = df_self15.sort_values( by = '買賣超金額', axis = 0, ascending = True )

    df_self15 = df_self15[ :15 ]
    # ------------------------------------------------------------------------------

    list_all_chip = df_sort[ ((df_sort[ '買進均價' ] > 0) | (df_sort[ '賣出均價' ] > 0)) & (df_sort[ '日期' ] == Start[ i ]) ][
        '券商' ]

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
    # 計算前15大買進均價
    # 計算前15大賣出均價
    # 計算前15大買超佔股本比
    # 計算前15大賣超佔股本比
    # ------------------------------------------------------------------------------

    df_tmp = pd.DataFrame(

        { '日期': start_date,

          '前15大買進均價': df_buy15[ '買賣超金額' ].sum( ) / df_buy15[ '買賣超' ].sum( ),

          '前15大買超佔股本比': df_buy15[ '買賣超金額' ].sum( ) / CapitalStock * 100,

          '前15大賣出均價': df_self15[ '買賣超金額' ].sum( ) / df_self15[ '買賣超' ].sum( ),

          '前15大賣超佔股本比': df_self15[ '買賣超金額' ].sum( ) / CapitalStock * 100,

          '前15大買賣超張數': df_buy15[ '買賣超' ].sum( ) - df_self15[ '買賣超' ].sum( ),

          '當日總卷商買家數': chip_buy_count,

          '當日總卷商賣家數': chip_self_count,

          '當日總卷商買賣家數': list_all_chip_count

          }, index = [ start_date ] )

    df_cal = pd.concat( [ df_cal, df_tmp ] )
    # -----------------------------------------------------------------------------

# 整組DataFrame根據index翻轉排序

df_cal = df_cal.iloc[ ::-1 ]
# ---------------------------------------------------------------------------------

df_freq_buy = df_freq_buy.value_counts( )[ :15 ]
df_freq_self = df_freq_self.value_counts( )[ :15 ]

df_writer = pd.ExcelWriter( tar_str + '.xls' )
df_sort.to_excel( df_writer, sheet_name = '籌碼分析' )
# ---------------------------------------------------

# 排序寫入欄位
# 日期範圍
# 前15大買超佔股本比
# 前15大賣超佔股本比
# 前15大買進均價
# 前15大賣出均價
# -------------------------------------------------

df_cal[ '股本' ] = CapitalStock

cols = [ '股本', '前15大買超佔股本比', '前15大賣超佔股本比', '前15大買賣超張數', '前15大買進均價', '前15大賣出均價',
         '當日總卷商買賣家數', '當日總卷商買家數', '當日總卷商賣家數' ]

df_cal = df_cal.reindex( columns = cols )

# ------------------------------------------------
# 取得收盤價
# ------------------------------------------------

df_cal.to_excel( df_writer, sheet_name = '買賣超金額15大' )
# ---------------------------------------------------------------

ti_a = qsp.Technical_Indicator( input_chip_str[ :4 ], 'A', 1400 )

ti_a.get_technical_indicator_dataframe( )

ti_a.df.to_excel( df_writer, sheet_name = '技術指標_還原日線' )
# ---------------------------------------------------------------

ti_d = qsp.Technical_Indicator( input_chip_str[ :4 ], 'D', 1400 )

ti_d.get_technical_indicator_dataframe( )

ti_d.df.to_excel( df_writer, sheet_name = '技術指標_日線' )
# ---------------------------------------------------------------

ti_w = qsp.Technical_Indicator( input_chip_str[ :4 ], 'W', 800 )

ti_w.get_technical_indicator_dataframe( )

ti_w.df.to_excel( df_writer, sheet_name = '技術指標_周線' )
# ---------------------------------------------------------------

ti_m = qsp.Technical_Indicator( input_chip_str[ :4 ], 'M', 300 )

ti_m.get_technical_indicator_dataframe( )

ti_m.df.to_excel( df_writer, sheet_name = '技術指標_月線' )
# ---------------------------------------------------------------

ti_60min = qsp.Technical_Indicator( input_chip_str[ :4 ], '60', 1200 )
#
ti_60min.get_technical_indicator_dataframe( )
#
ti_60min.df.to_excel( df_writer, sheet_name = '技術指標_60分線' )
# ---------------------------------------------------------------

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

tmp_1.columns = [ '累加買進次數' ]

tmp_1.sort_values( by = '累加買進次數', axis = 0, ascending = False, inplace = True )

tmp_2 = df_freq_self.to_frame( )

tmp_2.columns = [ '累加賣出次數' ]

tmp_2.sort_values( by = '累加賣出次數', axis = 0, ascending = False, inplace = True )

tmp_1.to_excel( df_writer, sheet_name = '買超券商' )

tmp_2.to_excel( df_writer, sheet_name = '賣超券商' )

df_writer.save( )