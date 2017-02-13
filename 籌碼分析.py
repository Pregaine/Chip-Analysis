# -*- coding: utf-8 -*-
import pandas as pd
import os
import re
import glob
import sys
# from datetime import datetime
import datetime

def foo( in_df ):

    for i in range( len( Start ) ):

        # start_date  = start_date_obj.strftime( '%Y%m%d' )
        # end_date    = end_date_obj.strftime( '%Y%m%d' )
        start_date  = Start[ i ].strftime( '%Y%m%d' )
        end_date    = End[ i ].strftime( '%Y%m%d' )


        mask = ( in_df[ '日期' ] >= start_date ) & ( in_df[ '日期'] <= end_date )

        df = in_df.loc[ mask ]

        try:
            # -------------------------------------------------------
            # 近幾日買進均價
            # -------------------------------------------------------

            in_df[ start_date + '~' + end_date + '買進均價' ] = df[ '買進價格*股數' ].sum( ) / df[ '買進股數' ].sum( )
        except:
            in_df[ start_date + '~' + end_date + '買進均價' ] = 0
            pass

        try:
            # -------------------------------------------------------
            # 近幾日賣出均價
            # -------------------------------------------------------

            in_df[ start_date + '~' + end_date + '賣出均價' ] = df[ '賣出價格*股數' ].sum( ) / df[ '賣出股數' ].sum( )
        except:
            in_df[ start_date + '~' + end_date + '賣出均價' ] = 0
            pass

        # -------------------------------------------------------
        # 近幾日買賣超
        # -------------------------------------------------------

        in_df[ start_date + '~' + end_date + '買賣超' ] = df[ '買賣超' ].sum( )

        # -------------------------------------------------------
        # 近幾日買賣超金額
        # -------------------------------------------------------

        in_df[ start_date + '~' + end_date + '買賣超金額' ] = df[ '買進價格*股數' ].sum( ) - df[ '賣出價格*股數' ].sum( )

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
    # input_df.drop( '?序號', axis=1, level=None, inplace=True )
    # input_df.drop( '價格', axis=1, level=None, inplace=True )
    # -------------------------------------------------------

    # --------------------------------------------------------
    # 新增欄位-日期
    # --------------------------------------------------------
    input_df['日期'] = date

    return input_df

def Cal_ChipDateList( Path, chip_str, end_date_str, cycle ):

    date_cycle = cycle
    start_date_list = [ ]
    end_date_list   = [ ]
    file_list       = [ ]
    date_cnt = 5
    theday_obj = datetime.datetime.strptime( end_date_str, '%Y%m%d' )
    # ----------------------------------------------------

    FilePath = Path + '全台卷商交易資料_' + end_date_str + '\\' + chip_str + '_' + end_date_str + '.csv'

    if os.path.exists( FilePath ):
        file_list.append( FilePath )
        end_date_list.append( theday_obj )
    else:
        print( chip_str + "結束日期無交易檔案")
        return

    while date_cycle > 0:

        prevday_obj = theday_obj - datetime.timedelta( days = 1 )
        prevday_str = prevday_obj.strftime( '_%Y%m%d' )

        FilePath = Path + '全台卷商交易資料' + prevday_str + '\\' + chip_str + prevday_str + '.csv'

        if os.path.exists( FilePath ):
            date_cnt -= 1

            if date_cnt == 1:
                start_date_list.append( prevday_obj )

            if date_cnt == 0:
                date_cycle -= 1
                date_cnt = 5

                if date_cycle > 0:
                    end_date_list.append( prevday_obj )

            if date_cycle > 0:
                file_list.append( FilePath )

        theday_obj = prevday_obj

    return start_date_list, end_date_list, file_list

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

df_sort = pd.DataFrame( )
df_cal  = pd.DataFrame( )
df_freq_buy = pd.Series( )
df_freq_self = pd.Series( )

# InputPath        =  sys.argv[ 1 ]
# input_chip_str   =  sys.argv[ 2 ]
# tar_str          =  sys.argv[ 3 ]
# end_date         =  sys.argv[ 4 ]
# cycle_chip         =  sys.argv[ 5 ]

InputPath        = 'D:\\03-workspace\\籌碼資料\\'
input_chip_str   = '1723中碳'
tar_str          = '1723中碳結果_170214'
end_date         = '20170208'
cycle_chip       = 10
CapitalStock     = 36920000000
# CapitalStock     =

Start, End, File = Cal_ChipDateList( InputPath, input_chip_str, end_date, cycle_chip )

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

    df = Chip_AddDate( df, YearPath.group( 0 ) )
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

    start_date = Start[ i ].strftime( '%Y%m%d' )
    end_date = End[ i ].strftime( '%Y%m%d' )

    df_sort.reset_index( 0, drop=True, inplace = True )

    # ------------------------------------------------------------------------------
    # 取出含有字串買賣超金額及券商的columns為另一個Dataframe
    # ------------------------------------------------------------------------------
    df_buy20 = df_sort[  df_sort[ start_date + '~' + end_date + '買賣超金額' ] > 0 ].copy( )
    del df_buy20[ start_date + '~' + end_date + '賣出均價' ]
    df_buy20.sort_values( by = start_date + '~' + end_date + '買賣超金額', axis = 0, inplace = True, ascending = False )
    df_buy20 = df_buy20[ :20 ]

    # print( '買超金額 > 0 ')
    # print( df_buy20 )
    #-------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------
    # 取出含有字串買賣超金額及券商的columns為另一個Dataframe
    # ------------------------------------------------------------------------------
    df_self20 = df_sort[  df_sort[ start_date + '~' + end_date + '買賣超金額' ] < 0 ].copy( )
    del df_self20[ start_date + '~' + end_date + '買進均價' ]
    df_self20.sort_values( by = start_date + '~' + end_date + '買賣超金額', axis = 0, inplace = True, ascending = True )
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

    df_tmp = pd.DataFrame( { '日期範圍' : start_date + '~' + end_date,
                         '前20大買進均價' : df_buy20[ start_date + '~' + end_date + '買賣超金額' ].sum( ) / df_buy20[ start_date + '~' + end_date + '買賣超' ].sum( ),
                         '前20大買超佔股本比' : df_buy20[ start_date + '~' + end_date + '買賣超金額' ].sum( ) / CapitalStock * 100,

                         '前20大賣出均價': df_self20[ start_date + '~' + end_date + '買賣超金額' ].sum( ) / df_self20[ start_date + '~' + end_date + '買賣超' ].sum( ),
                         '前20大賣超佔股本比' : df_self20[ start_date + '~' + end_date + '買賣超金額' ].sum( ) / CapitalStock * 100
                        }, index=[ 0 ] )

    df_cal = pd.concat( [ df_cal, df_tmp ] )

    #-----------------------------------------------------------------------------
df_cal[ '累積前20大買超佔股本比' ] = df_cal[ '前20大買超佔股本比' ].cumsum( )[::-1]
df_cal[ '累積前20大賣超佔股本比' ] = df_cal[ '前20大賣超佔股本比' ].cumsum( )[::-1]
#---------------------------------------------------------------------------------

df_freq_buy  = df_freq_buy.value_counts( )[ :20 ]
df_freq_self = df_freq_self.value_counts( )[ :20 ]

print( df_freq_buy )
print( df_freq_self )

df_writer = pd.ExcelWriter( tar_str + '.xlsx'  )

df_sort.to_excel( df_writer, sheet_name = '籌碼分析' )

df_cal.to_excel( df_writer, sheet_name = '買賣超金額20大' )

tmp_1 = df_freq_buy.to_frame( )

tmp_1.columns  = [ '累加買進次數' ]

tmp_2 = df_freq_self.to_frame( )

tmp_2.columns  = [ '累加賣出次數' ]

result = pd.concat( [ tmp_1, tmp_2 ], axis=1 )

result.to_excel( df_writer, sheet_name = '買賣超券商' )

df_writer.save( )

print( '完成' )