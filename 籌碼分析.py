# -*- coding: utf-8 -*-

import datetime
import os
import os.path
import re
import sys
import query_stock_price.query_stock_price_國票證券 as qsp
import chip_concentrate.chip_concentrate as cqy
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

InputPath = ".\\"
input_chip_str = "2330台積電"
tar_str = "2330台積電籌碼整理"
start_date = "20170601"
end_date = "20170609"
cycle_chip = 1
CapitalStock = 3530000000

# InputPath = sys.argv[ 1 ]
# input_chip_str = sys.argv[ 2 ]
# tar_str = sys.argv[ 3 ]
# start_date = sys.argv[ 4 ]
# end_date = sys.argv[ 5 ]
# cycle_chip = sys.argv[ 6 ]
# CapitalStock = sys.argv[ 7 ]

cycle_chip = int( cycle_chip )
CapitalStock = int( CapitalStock )

global_start_date = start_date

print( "股本", CapitalStock )

def remove_whitespace(x):
    """
    Helper function to remove any blank space from a string
    x: a string
    """
    try:
        # Remove spaces inside of the string
        x = "".join(x.split())

    except:
        pass
    return x

def Cal_ChipDateList( Path, chip_str, start_date_str, end_date_str ):

    file_list = [ ]
    time_obj_list = [ ]


    theday_obj = datetime.datetime.strptime( start_date_str, '%Y%m%d' )
    endday_obj = datetime.datetime.strptime( end_date_str, '%Y%m%d' )
    theday_str = theday_obj.strftime( '_%Y%m%d' )
    # ----------------------------------------------------

    while theday_obj <= endday_obj:

        FilePath = Path + '全台卷商交易資料' + theday_str + '\\' + chip_str + theday_str + '.csv'
        print( FilePath )

        if os.path.exists( FilePath ):
            print( "yes" )
            file_list.append( FilePath )
            time_obj_list.append( theday_obj )

        theday_obj += datetime.timedelta( days = 1 )
        theday_str = theday_obj.strftime( '_%Y%m%d' )

    return time_obj_list, file_list

Tiem_Obj, File = Cal_ChipDateList( InputPath, input_chip_str, start_date, end_date )

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
    df = pd.read_csv( filename_str, sep = ',', encoding = 'utf8', false_values = 'NA',
                      names = [ '序號', '券商', '價格', '買進股數', '賣出股數' ] )
    # --------------------------------------------------------

    df[ '日期' ] = YearPath.group( 0 )
    df_sort = pd.concat( [ df_sort, df ] )

print( df_sort )

df_sort = df_sort[ df_sort[ '券商' ].notnull( ) ]

df_sort[ '券商' ] = df_sort[ '券商' ].str.replace('\s+', '')

df_sort[ '買進價格*股數' ] = df_sort[ '買進股數' ] * df_sort[ '價格' ]
df_sort[ '賣出價格*股數' ] = df_sort[ '賣出股數' ] * df_sort[ '價格' ]
# --------------------------------------------------------

df_sort.drop( '價格', axis = 1, level = None, inplace = True )
df_sort.drop( '序號', axis = 1, level = None, inplace = True )
# -------------------------------------------------------

df_sort[ '日期' ] = pd.to_datetime( df_sort[ '日期' ], format = '%Y%m%d' )

df_sort = df_sort.groupby( [ '券商', '日期' ] ).sum( ).reset_index( )

df_sort[ '買進均價' ] = df_sort[ '買進價格*股數' ] / df_sort[ '買進股數' ]
df_sort[ '賣出均價' ] = df_sort[ '賣出價格*股數' ] / df_sort[ '賣出股數' ]

df_sort[ '買進張數' ] = df_sort[ '買進股數' ] / 1000
df_sort[ '賣出張數' ] = df_sort[ '賣出股數' ] / 1000

df_sort[ '買進價格*張數' ] = df_sort[ '買進價格*股數' ] / 1000
df_sort[ '賣出價格*張數' ] = df_sort[ '賣出價格*股數' ] / 1000

df_sort[ '買賣超金額' ] = df_sort[ '買進價格*股數' ] - df_sort[ '賣出價格*股數' ]

df_sort[ '買賣超張數' ] = df_sort[ '買進張數' ] - df_sort[ '賣出張數' ]

df_sort[ '買賣超股數' ] = df_sort[ '買進股數' ] - df_sort[ '賣出股數' ]

df_sort[ '買賣超佔股本比' ] = df_sort[ '買賣超金額' ] / CapitalStock

df_sort[ '股本' ] = CapitalStock

# -------------------------------------------------
# 取得收盤價
# ------------------------------------------------

ti_a = qsp.Technical_Indicator( input_chip_str[ :4 ], 'A', 1400 )

ti_a.get_technical_indicator_dataframe( )
# ---------------------------------------------------------------

ti_d = qsp.Technical_Indicator( input_chip_str[ :4 ], 'D', 1400 )

ti_d.get_technical_indicator_dataframe( )
# ---------------------------------------------------------------

ti_w = qsp.Technical_Indicator( input_chip_str[ :4 ], 'W', 800 )

ti_w.get_technical_indicator_dataframe( )
# ---------------------------------------------------------------

ti_m = qsp.Technical_Indicator( input_chip_str[ :4 ], 'M', 300 )

ti_m.get_technical_indicator_dataframe( )
# ---------------------------------------------------------------

ti_60min = qsp.Technical_Indicator( input_chip_str[ :4 ], '60', 1200 )
#
ti_60min.get_technical_indicator_dataframe( )
# ---------------------------------------------------------------

cols = [ '券商', '日期', '買進均價', '賣出均價', '買進張數', '賣出張數', '買賣超張數', '買賣超股數', '買進價格*張數', '賣出價格*張數',
         '買賣超金額', '買賣超佔股本比', '股本', '收盤', '成交量' ]

tmp = ti_a.df.loc[ :, [ '日期', '收盤', '成交量' ] ]

tmp[ '日期' ] = pd.to_datetime( tmp[ '日期' ], format = '%Y/%m/%d' )

df_sort = pd.merge( tmp, df_sort, how = 'inner', on = '日期' )

df_sort = df_sort.reindex( columns = cols )
# ---------------------------------------------------------------------------------

Day_1_Obj = cqy.Chip_Concentrate( df_sort, Tiem_Obj, interval_day = 1 )

Day_1_df = Day_1_Obj.sort_source( )

Day_5_Obj = cqy.Chip_Concentrate( df_sort, Tiem_Obj, interval_day = 5 )

Day_5_df = Day_5_Obj.sort_source( )

Day_10_Obj = cqy.Chip_Concentrate( df_sort, Tiem_Obj, interval_day = 10 )

Day_10_df = Day_10_Obj.sort_source( )

Day_20_Obj = cqy.Chip_Concentrate( df_sort, Tiem_Obj, interval_day = 20 )

Day_20_df = Day_20_Obj.sort_source( )

Day_30_Obj = cqy.Chip_Concentrate( df_sort, Tiem_Obj, interval_day = 30 )

Day_30_df = Day_30_Obj.sort_source( )

Day_45_Obj = cqy.Chip_Concentrate( df_sort, Tiem_Obj, interval_day = 45 )

Day_45_df = Day_45_Obj.sort_source( )

Day_60_Obj = cqy.Chip_Concentrate( df_sort, Tiem_Obj, interval_day = 60 )

Day_60_df = Day_60_Obj.sort_source( )

Day_120_Obj = cqy.Chip_Concentrate( df_sort, Tiem_Obj, interval_day = 120 )

Day_120_df = Day_120_Obj.sort_source( )

Day_240_Obj = cqy.Chip_Concentrate( df_sort, Tiem_Obj, interval_day = 240 )

Day_240_df = Day_240_Obj.sort_source( )
# ---------------------------------------------------

df_writer = pd.ExcelWriter( tar_str + '.xlsx' )
df_sort.to_excel( df_writer, sheet_name = '籌碼分析' )

# Day_1_df[ '1日集中度' ] = Day_1_Obj.get_concentration_ratio( )
Day_1_df[ '5日集中度' ]   = Day_5_Obj.get_concentration_ratio( )
Day_1_df[ '10日集中度' ]  = Day_10_Obj.get_concentration_ratio( )
Day_1_df[ '20日集中度' ]  = Day_20_Obj.get_concentration_ratio( )
Day_1_df[ '30日集中度' ]  = Day_30_Obj.get_concentration_ratio( )
Day_1_df[ '45日集中度' ]  = Day_45_Obj.get_concentration_ratio( )
Day_1_df[ '60日集中度' ]  = Day_60_Obj.get_concentration_ratio( )
Day_1_df[ '120日集中度' ] = Day_120_Obj.get_concentration_ratio( )
Day_1_df[ '240日集中度' ] = Day_240_Obj.get_concentration_ratio( )

Day_1_df.to_excel( df_writer, sheet_name = '1日買賣超金額15大' )
# Day_5_df.to_excel( df_writer, sheet_name = '5日買賣超金額15大' )
# Day_10_df.to_excel( df_writer, sheet_name = '10日買賣超金額15大' )
# Day_20_df.to_excel( df_writer, sheet_name = '20日買賣超金額15大' )
# Day_30_df.to_excel( df_writer, sheet_name = '30日買賣超金額15大' )
# Day_45_df.to_excel( df_writer, sheet_name = '45日買賣超金額15大' )
# Day_60_df.to_excel( df_writer, sheet_name = '60日買賣超金額15大' )
# Day_120_df.to_excel( df_writer, sheet_name = '120日買賣超金額15大' )
# Day_240_df.to_excel( df_writer, sheet_name = '240日買賣超金額15大' )

ti_60min.df.to_excel( df_writer, sheet_name = '技術指標_60分線' )
ti_a.df.to_excel( df_writer, sheet_name = '技術指標_還原日線' )
ti_d.df.to_excel( df_writer, sheet_name = '技術指標_日線' )
ti_w.df.to_excel( df_writer, sheet_name = '技術指標_周線' )
ti_m.df.to_excel( df_writer, sheet_name = '技術指標_月線' )

df_writer.save( )
# ---------------------------------------------------------------

# df_freq_buy = df_freq_buy.value_counts( )[ :15 ]
# df_freq_self = df_freq_self.value_counts( )[ :15 ]
#
# tmp_1 = df_freq_buy.to_frame( )
#
# tmp_1.columns = [ '累加買進次數' ]
#
# tmp_1.sort_values( by = '累加買進次數', axis = 0, ascending = False, inplace = True )
#
# tmp_2 = df_freq_self.to_frame( )
#
# tmp_2.columns = [ '累加賣出次數' ]
#
# tmp_2.sort_values( by = '累加賣出次數', axis = 0, ascending = False, inplace = True )
#
# tmp_1.to_excel( df_writer, sheet_name = '買超券商' )
#
# tmp_2.to_excel( df_writer, sheet_name = '賣超券商' )
