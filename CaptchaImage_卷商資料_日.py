# coding: utf-8

import matplotlib.pyplot as plt
import shutil
import requests
import re
import pandas as pd
import csv
import os
import time
import urllib.parse

#-----------------------------------------------------------------------
#傳入清單,回傳內容無重覆清單
#-----------------------------------------------------------------------
def remove_duplicates( l ):
    return list( set( l ) )
#-----------------------------------------------------------------------
	
#------------------------------------------------------------------------
#網頁頭參數
#詢問得到以下參數
#viewstate, eventvalidation,
#------------------------------------------------------------------------
headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0.1) Gecko/2010010' \
    '1 Firefox/4.0.1',
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language':'en-us,en;q=0.5',
    'Accept-Charset':'ISO-8859-1,utf-8;q=0.7,*;q=0.7'}

rs = requests.session()

res = rs.get( 'http://bsr.twse.com.tw/bshtm/bsMenu.aspx', stream = True, verify = False, headers =headers )

# print( res.cookies )
# print(res.text)

viewstate = re.search( 'VIEWSTATE"\s+value=.*=', res.text )
viewstate = viewstate.group( )[18:]

eventvalidation = re.search( 'EVENTVALIDATION"\s+value=.*\w', res.text )
eventvalidation = eventvalidation.group()[24:]

# encode_viewstate = urllib.parse.urlencode( { viewstate : '' }  )
# print( encode_viewstate[:-1] )
# encode_eventvalidation = urllib.parse.urlencode( { eventvalidation : '' } )
# print( encode_eventvalidation[:-1] )
#------------------------------------------------------------------------

#------------------------------------------------------------------------
#搜尋網頁回應內容關鍵字'CaptchaImage.*guid+\S*\w'
#根據關鍵字獲得驗證碼圖片
#------------------------------------------------------------------------
str = re.search( 'CaptchaImage.*guid+\S*\w', res.text )

res = rs.get( 'http://bsr.twse.com.tw/bshtm/' + str.group( ), stream = True, verify = False ) 

f = open( 'check.png', 'wb' )
shutil.copyfileobj( res.raw, f )
f.close

print( 'http://bsr.twse.com.tw/bshtm/' + str.group() )
#------------------------------------------------------------------------

#------------------------------------------------------------------------
#讀取csv file,得到股號清單
#------------------------------------------------------------------------
filename = './/revenue_sii_201701'
revenu_pd = pd.read_csv( filename + '.csv', encoding = 'utf8' )
stock_code_list = revenu_pd['公司代號'].values.tolist()
stock_code_list = remove_duplicates(stock_code_list)
#------------------------------------------------------------------------


#------------------------------------------------------------------------
#初始化參數
#------------------------------------------------------------------------
num = 0
headers = {'User-Agent': 'Mozilla/5.0'}
#------------------------------------------------------------------------


#------------------------------------------------------------------------
#根據股號清單,詢問網頁
#------------------------------------------------------------------------
for index in range( len( stock_code_list ) ):
    
	#詢問股號,網頁無回應或回應錯誤次數
    miss_cnt = 0
        
    code_str = '{}'.format( stock_code_list[index] )    
    
    date = None
       
    while( date == None and miss_cnt < 20 ):
        
        rs = requests.session()
        miss_cnt = miss_cnt + 1
        
        time.sleep(2)
        res = rs.get( 'http://bsr.twse.com.tw/bshtm/bsMenu.aspx', stream = True, verify = False, headers = headers, timeout=None )
        print( '股號', stock_code_list[index], 'Response', res.status_code, index )

		
		#----------------------------------------------------------------------------------
		#根據網頁響應內容"res.text"
		#取出參數viewstate, eventvalidation
        #----------------------------------------------------------------------------------
        try:
            viewstate = re.search( 'VIEWSTATE"\s+value=.*=', res.text )
            viewstate = viewstate.group()[18:]
            eventvalidation = re.search( 'EVENTVALIDATION"\s+value=.*\w', res.text )
            eventvalidation = eventvalidation.group()[24:]
        except:
            continue
        
		#----------------------------------------------------------------------------------
		#根據網頁響應內容"res.text"
		#根據參數viewstate, eventvalidation
		#得到個股卷商交易資料"date"
        #----------------------------------------------------------------------------------
        str = re.search('CaptchaImage.*guid+\S*\w', res.text )
        str.group()
        res = rs.get( 'http://bsr.twse.com.tw/bshtm/' + str.group(), stream = True, verify = False ) 
		#f = open('check.png', 'wb')
		#shutil.copyfileobj( res.raw, f )
		#f.close
        #----------------------------------------------------------------------------------
		
        payload = {
        '__EVENTTARGET':'',
        '__EVENTARGUMENT':'',
        '__LASTFOCUS':'',
        '__VIEWSTATE' : viewstate,                      #encode_viewstate[:-1],
        '__EVENTVALIDATION' : eventvalidation,          #encode_eventvalidation[:-1],
        'RadioButton_Normal' : 'RadioButton_Normal',
        'TextBox_Stkno' : code_str,
        'CaptchaControl1 ' : 'Z67YA',
        'btnOK' : '%E6%9F%A5%E8%A9%A2',
        }

        rs.post( "http://bsr.twse.com.tw/bshtm/bsMenu.aspx", data=payload, headers=headers, verify = False, stream = True )
        res = rs.get( 'http://bsr.twse.com.tw/bshtm/bsContent.aspx?v=t',verify = False ,stream = True, )
        date = re.search( 'receive_date.*\s.*\d', res.text )
        #print( '股號', stock_code_list[index], 'Response', res.status_code, '內容' )
        
    #----------------------------------------------------------------------------------    
	#查詢次數已達到20次,未成功切換下檔股號
	#----------------------------------------------------------------------------------
    if( miss_cnt == 20 ):
        continue

	#----------------------------------------------------------------------------------	
	#前面手續為模擬詢問卷商資料過程
	#成功,代表此Sesion可以得到正確響應內容
	#----------------------------------------------------------------------------------
    date = date.group()[-10:].replace('/', '')
    name = re.search( '&nbsp;.*<', res.text )
    name = name.group()[6:-1]
    #----------------------------------------------------------------------------------
    
	
	#----------------------------------------------------------------------------------
	#詢問網址"http://bsr.twse.com.tw/bshtm/bsContent.aspx"
	#得到正確卷商交易資料,存到清單"raw"
	#----------------------------------------------------------------------------------
    tmp_csv = rs.get( 'http://bsr.twse.com.tw/bshtm/bsContent.aspx', verify = False ,stream = True ) 
    
    while( tmp_csv.status_code > 300 or  tmp_csv.status_code < 200 ):
        tmp_csv = rs.get( 'http://bsr.twse.com.tw/bshtm/bsContent.aspx', verify = False ,stream = True )   
    
    data = tmp_csv.text.split( '\n' )
    rows = list()

    for row in data:
        if ',,' in row:
            i = re.search( '.*,,', row )
            rows.append( i.group()[:-2] )

            i = re.search( ',,.*\s', row )
            rows.append( i.group()[2:-1] )
        else:
            rows.append( row )
    
	#----------------------------------------------------------------------------------
	#斷開連結,斷開鎖鍊
	#----------------------------------------------------------------------------------
    rs.close()

	#-------------------------------------
	#初始化資料夾名稱
	#-------------------------------------
    Savefiledir = '.\\'+ date + '籌碼\\' 
    
	#-------------------------------------
	#若無資料夾,建立資料夾
	#-------------------------------------
    if not os.path.isdir(Savefiledir):
        os.makedirs(Savefiledir)
        
	#-------------------------------------
	#列印即將寫入檔名
	#-------------------------------------
    print( Savefiledir + payload['TextBox_Stkno'] + name + '.csv' )

	#-------------------------------------
	#根據清單"raw"檔案寫入csv
	#-------------------------------------
    with open( Savefiledir + payload['TextBox_Stkno'] + name + '.csv', 'w', newline='\n', encoding='utf-8' )as file:
        w = csv.writer( file )
        w.writerow( [ '序號', '券商', '價格', '買進股數', '賣出股數' ] )
        for data in rows[4:]:
            s = data.split( ',' )
            w.writerow( s )




