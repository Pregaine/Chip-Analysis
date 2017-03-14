from bs4 import BeautifulSoup as BS
import requests
import os
import pandas as pd
import time
from random import randint
from stem import Signal
from stem.control import Controller

#-----------------------------------------------
#查詢IP網址
#-----------------------------------------------
url = 'https://api.ipify.org?format=json'
#-----------------------------------------------

#-----------------------------------------------
#使用Controller Module控制Port
#要求Tor更換新IP
#-----------------------------------------------
def new_ip( ):
    with Controller.from_port( port = 9151 ) as controller:
        controller.authenticate( 'pregaine' )
        controller.signal( Signal.NEWNYM )
#-----------------------------------------------
        
#-----------------------------------------------
#使用Tor連入本機127.0.0.1，換IP連線連出外網
#更換IP
#-----------------------------------------------
def getip_requesocks( url ):
    
    new_ip( )
    print( "(+) Sending request with requesocks..." )
    session = requests.session( )
    session.proxies = { 'http' : 'socks5://127.0.0.1:9050',
                        'https': 'socks5://127.0.0.1:9050' }
    r = session.get( url )
    print( "(+) IP is: " + r.text.replace( "\n", "" ) )
    return session
#-----------------------------------------------

#-----------------------------------------------
#存檔資料夾定義
#-----------------------------------------------
Savefiledir = '.\\合併財務比率累計季表資料_sii\\'
if not os.path.isdir( Savefiledir ):
    os.makedirs( Savefiledir )
#-----------------------------------------------
    
#-----------------------------------------------
#從goodinfo網頁下拉季範圍清單獲取，產生清單
#-----------------------------------------------    
def RETURN_TIME_LIST( number_str, conect ):

    url = "http://goodinfo.tw/StockInfo/StockFinDetail.asp"
    querystring = { "RPT_CAT": "XX_M_QUAR_ACC", "STOCK_ID": number_str }
    headers = {
        'upgrade-insecure-requests' : "1",
        'user-agent'                : "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36",
        'accept-language'           : "zh-TW,zh-CN;q=0.8,zh;q=0.6,en-US;q=0.4,en;q=0.2",
    }

    response = conect.request( "GET", url, headers = headers, params = querystring )

    response.encoding = 'utf8'
    # print( response.text )

    time.sleep( randint( 1, 2 ) )

    cookie_str = response.headers['Set-Cookie'][:-8]
    soup = BS( response.text, "html.parser" )
    # -----------------------------------------------------------------------------

    QRY_TIME = soup.select( '#QRY_TIME' )
    children = QRY_TIME[0].findChildren()

    ALLQ_LEN = len( children )
    ALLQ_LIST = [ children[0]['value'] ]

    if( ALLQ_LEN > 10 ):
        ALLQ_LEN = ALLQ_LEN - 10
        ALLQ_LIST.append( children[10]['value'] )

    if( ALLQ_LEN > 10 ):
        ALLQ_LEN = ALLQ_LEN - 10
        ALLQ_LIST.append( children[20]['value'] )

    if( ALLQ_LEN > 10 ):
        ALLQ_LEN = ALLQ_LEN - 10
        ALLQ_LIST.append( children[30]['value'] )

    if( ALLQ_LEN > 10 ):
        ALLQ_LEN = ALLQ_LEN - 10
        ALLQ_LIST.append( children[40]['value'] )

    return ALLQ_LIST, cookie_str
#-----------------------------------------------------------------------------

#-----------------------------------------------
#整理獲取個股網頁內容，損益表/現金流量/資產負債表/財務比率表
#-----------------------------------------------
def sort_dataframe( receiver_data, rpt_cat ):
    ratio = pd.DataFrame( )
    year_list = []
    value = []
    soup = BS( receiver_data, "html.parser" )

    # print( soup )
    r = soup.find( 'tr', { "bgcolor": "#EBF7FF" } )
    for td in r.find_all( 'td' )[1:]:
        year_list.append( td.get_text( ) )

    ratio['年份'] = year_list
    print( year_list )

    data = soup.find_all( 'tr', attrs = { 'bgcolor':'white', 'align':'right', 'valign':'middle', 'height':'25px'  } )

    for row in data[:-1]:
        key = row.findChildren( 'td' )

        if( rpt_cat is 'BS_M_QUAR' or rpt_cat is 'IS_M_QUAR_ACC' ):
            for data in key[ 1::2 ]:
                try:
                    value.append( float( data.get_text( ) ) )
                except ValueError:
                    value.append( 0 )
            # print( key[0].get_text( ) + '金額', value )
            ratio[ key[0].get_text( ) + '金額' ] = value
            value.clear( )

            for data in key[ 2::2 ]:
                try:
                    value.append( float( data.get_text( ) ) )
                except ValueError:
                    value.append( 0 )
            # print( key[0].get_text( ) + '%', value )
            ratio[ key[0].get_text( ) + '%' ] = value
            value.clear( )
        else:
            for data in key[ 1: ]:
                try:
                    value.append( float( data.get_text( ) ) )
                except ValueError:
                    value.append( 0 )
            # print( key[0].get_text( ), value )
            ratio[ key[0].get_text( ) ] = value
            value.clear( )

    return ratio
#-----------------------------------------------------------------------------

#-----------------------------------------------
#傳入年份，個股，查詢財務比例/現金流量/負債/損益表參數，連線參數
#獲取網頁內容
#-----------------------------------------------
def surf_internet( year, number, rpt_cat, conect ):
    url         = "http://goodinfo.tw/StockInfo/StockFinDetail.asp"
    querystring = { "STEP": "DATA", "STOCK_ID": number, "RPT_CAT": rpt_cat, "QRY_TIME": year }
    headers = {
        'referer' : "http://goodinfo.tw/StockInfo/StockFinDetail.asp?RPT_CAT=" + rpt_cat + "&STOCK_ID=" + number,
        'upgrade-insecure-requests': "1",
        'user-agent'               : "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36",
        'accept-language'          : "zh-TW,zh-CN;q=0.8,zh;q=0.6,en-US;q=0.4,en;q=0.2",
    }

    response = conect.request( "POST", url, headers = headers, params = querystring )
    response.encoding = 'utf8'
    return response.text
#-----------------------------------------------------------------------------


#-----------------------------------------------
#傳入年份清單，個股股號，查詢表，連線參數
#整理獲取個股內容，存成DataFrame
#-----------------------------------------------
def Save_EXCEL( list, number_str, rpt_cat, conect ):
    frames = [ ]
    for QRY in list:
        print( QRY, number_str, rpt_cat, "傳入字串" )
        text = surf_internet( QRY, number_str,rpt_cat, conect )
        A = sort_dataframe( text, rpt_cat  )
        frames.append( A )
        time.sleep( 0.5 )

    result = pd.concat( frames )
    return result
#---------------------------------------------------------------------------------

#-----------------------------------------------
#移除清單中重覆資料
#-----------------------------------------------
def remove_duplicates( l ):
    return list( set( l ) )
#-----------------------------------------------
 
#----------------------------------------------- 
#根據csv檔，取得查詢個股清單    
#-----------------------------------------------    
def get_list( ):
    filename = 'revenue_sii_201609.csv'
    revenu_pd  = pd.read_csv( filename, encoding = 'utf8' )
    stock_code = revenu_pd['公司代號'].values.tolist( )
    stock_code = remove_duplicates( stock_code )
    return  stock_code
#-----------------------------------------------
    
#-----------------------------------------------
#初始化參數並獲取個股清單    
#---------------------------------------------------------------------------------
num = 0
stock_code_list = get_list( )
#-----------------------------------------------

#-----------------------------------------------
#根據個股清單，查詢網頁
#將獲取的內容DataFrame存成excel各項表格
#財務比率表/現金流量表/損益表/負債表
#-----------------------------------------------
for code in stock_code_list:
    
    print( '股號', code, '剩餘', len( stock_code_list ) - num )
    num += 1

    #-------------------------------
    #檢查個股資料表存在，則換查詢下個個股
    #-------------------------------
    if os.path.isfile( Savefiledir + str( code ) + '.xlsx' ):
        continue
    #------------------------------

    #---------------------------------
    #換IP
    #---------------------------------
    session_con = getip_requesocks( url )
    #---------------------------------
    
    # try:
    ALLQ, COOKIE = RETURN_TIME_LIST( str( code ), session_con )
    XX_M = Save_EXCEL( ALLQ, str( code ), 'XX_M_QUAR_ACC', session_con  )
    CF_M = Save_EXCEL( ALLQ, str( code ), 'CF_M_QUAR_ACC', session_con )
    IS_M = Save_EXCEL( ALLQ, str( code ), 'IS_M_QUAR_ACC', session_con )
    BS_M = Save_EXCEL( ALLQ, str( code ), 'BS_M_QUAR',  session_con )

    stock_writer = pd.ExcelWriter( Savefiledir + str( code ) + '.xlsx' )
    XX_M.to_excel( stock_writer, sheet_name = '財務比率表', encoding = 'utf8' )
    CF_M.to_excel( stock_writer, sheet_name = '現金流量表', encoding = 'utf8' )
    IS_M.to_excel( stock_writer, sheet_name = '損益表', encoding = 'utf8' )
    BS_M.to_excel( stock_writer, sheet_name = '資產負債表', encoding = 'utf8' )

    stock_writer.save( )

    # except:
    #     print( '股號', code, '未知錯誤' )