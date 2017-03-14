# coding: utf-8

import pandas as pd
import requests
from bs4 import BeautifulSoup as BS
import csv
import os
import time

#=======================================================================
#移除重複資料
#=======================================================================
def remove_duplicates( l ):
    return list( set( l ) )
#=======================================================================

#=======================================================================
#獲取股號清單
#=======================================================================
filename = './/revenue_sii_201701.csv'
revenu_pd  = pd.read_csv( filename, encoding = 'utf8' )
stock_list = revenu_pd[ '公司代號' ].values.tolist( )
stock_list = remove_duplicates(stock_list)
#print(stock_list)
#print( 'len = ', len(stock_list ) )
#=======================================================================

#=======================================================================
#若資料夾無，則建立
#=======================================================================
Savefiledir = '.\\集保戶股權分散表_sii\\'  

if not os.path.isdir( Savefiledir ):
    os.makedirs( Savefiledir )
#=======================================================================

#=======================================================================
#初始化參數
#=======================================================================
date_list = []
num = 0
num_all = len( stock_list )
#=======================================================================

#=======================================================================
#詢問網頁，獲得周清單
#=======================================================================
res  = requests.get( 'http://www.tdcc.com.tw/smWeb/QryStock.jsp' )
soup = BS( res.text, "html.parser" )
time.sleep( 2 )

for date in soup.find_all( 'option' ):
    date = '{}'.format( date.text )
    date_list.append( date )
#=======================================================================
  	
#=======================================================================
#根據周及股號詢問網頁獲取集保分佈
#寫入CSV檔"w"
#=======================================================================	
def save_csv( date ):
    code = 'SCA_DATE=' + date + '&SqlMethod=StockNo&StockNo=' + stock + '&StockName=&sub=%ACd%B8%DF'
    res = requests.get( 'http://www.tdcc.com.tw/smWeb/QryStock.jsp?' + code )
    soup = BS( res.text )    

    tb = soup.select('.mt')[1]
    name = soup.findAll( 'td', class_ = 'bw09' )[0]
    name = ( name.text[-2:] )
     
    for tr in tb.select('tr'):
        if( tr.select('td')[0].text == '序' ):
            continue
            
        units_grading = tr.select('td')[1].text.replace(',','')
        people        = tr.select('td')[2].text.replace(',','')
        femoral       = tr.select('td')[3].text.replace(',','')
        ratio         = tr.select('td')[4].text.replace(',','')
        rvnlst=( name, date,tr.select('td')[0].text, units_grading, people, femoral, ratio )
        w.writerow( rvnlst )
#=======================================================================#

#=======================================================================
#根據股號跑迴圈
#=======================================================================
for stock in stock_list:
    stock = '{}'.format( stock )
    num = num + 1
    print( '股號', stock, '剩下', num_all - num )
    
	#=======================================================================
	#寫入第一列資料( '','','序','持股/單位數分級', '人數', '股數/單位數', '佔集保庫存數比例(%)' )
	#=======================================================================
    with open( Savefiledir + '集保股號'+ stock + '.csv', 'w', newline='', encoding='utf-8' )as file:
        w = csv.writer( file )
        w.writerow( ( '','','序','持股/單位數分級', '人數', '股數/單位數', '佔集保庫存數比例(%)' ) )
        
        for date in date_list:
            save_csv( date )
            time.sleep(0.2)
 #=======================================================================