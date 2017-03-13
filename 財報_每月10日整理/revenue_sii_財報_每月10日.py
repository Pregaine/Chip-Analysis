# coding: utf-8

import urllib.request  
from bs4 import BeautifulSoup
import csv
import os
  
Savefiledir = '.\\Revenue_sii_CSV\\'  

if not os.path.isdir(Savefiledir):
    os.makedirs( Savefiledir )
  
for j in range( 2010, 2017 ):
    for k in range( 1, 13 ):
        pyROCYear = str( j - 1911 )
    
        ym = str( j ) + str( '%02d' %k )      
        #------------------------------------------------------------
        #累計與當月營業收入統計表網址
        #------------------------------------------------------------
        url = 'http://mops.twse.com.tw/nas/t21/sii/t21sc03_'+ pyROCYear + '_' + str( k ) + '_0.html'  
        response = urllib.request.urlopen( url )  
        html = response.read()  
        sp = BeautifulSoup( html.decode('cp950','ignore').encode('utf-8'), "html.parser" )   
      
        tblh = sp.find_all( 'table', attrs = { 'border' : '0','width' : '100%' } )  
        
        #----------------------
        #列印查詢年，月
        #----------------------
        print( ym )  
    
        with open( Savefiledir + 'revenue_sii_' + ym +'.csv', 'w', newline='', encoding='utf-8' )as file:
            w = csv.writer( file )
            
            #---------------------------------------
            #寫入第一欄位內容如下
            #---------------------------------------
            w.writerow( [ u'產業別', u'公司代號', u'公司名稱', u'當月營收', u'上月營收', u'去年當月營收', u'上月比較增減(%)', u'去年同月增減(%)', u'當月累計營收', u'去年累計營收', u'前期比較增減(%)'] )
    
            for h in range( 0, len( tblh ) ):  
                
                th  = tblh[ h ].find( 'th', attrs={ 'align' : 'left','class' : 'tt' } )  
                cls = th.get_text().split('：') #產業別  
                tbl = tblh[h].find( 'table', attrs={ 'bordercolor' : "#FF6600" } )  
                trs = tbl.find_all( 'tr' )  
                
                for r in range( 0, len( trs ) ):  
                    if r>1 and r<(len(trs)-1):  
                        tds=trs[r].find_all( 'td' )  
                        td0=tds[0].get_text()  
                        td1=tds[1].get_text()  
                        td2=tds[2].get_text().strip().replace( ",", "" )#
                        td3=tds[3].get_text().strip().replace( ",", "" )#  
                        td4=tds[4].get_text().strip().replace( ",", "" )#  
                        td5=tds[5].get_text().strip().replace( ",", "" )#
                        td6=tds[6].get_text().strip().replace( ",", "" )#
                        td7=tds[7].get_text().strip().replace( ",", "" )#  
                        td8=tds[8].get_text().strip().replace( ",", "" )#  
                        td9=tds[9].get_text().strip().replace( ",", "" )  
  
                        rvnlst=( cls[1],td0,td1,td2,td3,td4,td5,td6,td7,td8,td9 )   
                        w.writerow( rvnlst )