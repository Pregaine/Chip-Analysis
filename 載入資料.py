# -*- coding: utf-8 -*-
import glob
import os
import re
import csv


InputPath = "D:\\03-workspace\\02-卷商分點\\搜集籌碼資料\\**\\"

InputFilePathList = [ ]

for input_file in glob.glob( os.path.join( InputPath, '*.csv') ):

    # print( input_file )

    InputFilePathList.append( input_file )
# ---------------------------------------------------

# ---------------------------------------------------
# 讀取路徑年份
# 開啟csv檔案
# 加入欄位年月日
# 另存新檔名-股號_年月日.csv
# ---------------------------------------------------

for filepath in InputFilePathList:

    re_obj = re.compile( r'[0-9]{8}' )
    YearPath = re_obj.search( filepath )

    re_obj = re.compile( r'籌碼\\[0-9]{4}\S+' )
    filename = re_obj.search( filepath )

    filename = filename.group( 0 )
    filename_str = filename.replace( '籌碼\\', '' )
    filename_str = filename_str.replace( '.csv', '' )

    print( 'Year', YearPath.group( 0 ) )
    print( 'filename', filename_str )

    output_path = 'D:\\03-workspace\\02-卷商分點\\搜集籌碼資料\\'
    output_file = output_path + filename_str + '_' + YearPath.group( 0 ) + '.csv'

    columns = [ 0, 1, 2, 3, 4, ]

    with open( filepath, 'r', newline = '', encoding = 'utf8' ) as csv_in_file:
        with open( output_file, 'w', newline = '' ) as csv_out_file:
            filereader = csv.DictReader( csv_in_file, delimiter = ',' )
            filewriter = csv.writer( csv_out_file, delimiter = ',' )

            for row_list in filereader:
                row = ( row_list[ '序號' ], row_list[ '券商' ], row_list[ '價格'], row_list[ '買進股數' ], row_list[ '賣出股數' ] )
                filewriter.writerow( row )

    print( '完成' )




