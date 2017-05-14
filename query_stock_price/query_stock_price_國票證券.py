import requests
import numpy as np
import pandas as pd
import talib

class Technical_Indicator:

    """ 輸入線型周期, 查詢股價
        D 日線
        W 週線
        M 月線
        A 還原日線
        5  5分鐘
        10 10分鐘
        30 30分鐘 
        60 60分鐘
        """

    def __init__( self, number = '2330', query_value = 'A', days = 5  ):

        self.number = number
        self.query_value = query_value
        self.days = days
        self.df = pd.DataFrame( )

        query_dict = dict( )

        query_dict[ 'D' ] = '日線'
        query_dict[ 'W' ] = '周線'
        query_dict[ 'M' ] = '月線'
        query_dict[ 'A' ] = '還原日線'
        query_dict[ '5' ] = '5分鐘'
        query_dict[ '10' ] = '10分鐘'
        query_dict[ '30' ] = '30分鐘'
        query_dict[ '60' ] = '60分鐘'

        if query_value not in query_dict.keys( ):
            raise ValueError( "輸入錯誤周期" )

        j = 0
        url = "http://jsstock.wls.com.tw/Z/ZC/ZCW/CZKC1.djbcd"
        querystring = { "a": self.number, "b": self.query_value, "c": self.days }

        headers = { 'upgrade-insecure-requests': "1", 'user-agent': "Chrome/58.0.3029.110 Safari/537.36",
                    'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    'accept-encoding': "gzip, deflate, sdch",
                    'accept-language': "zh-TW,zh-CN;q=0.8,zh;q=0.6,en-US;q=0.4,en;q=0.2", 'cache-control': "no-cache",
                    'postman-token': "bd621a32-aa17-3f07-614e-487a1c7100de" }

        response = requests.request( "GET", url, headers = headers, params = querystring )

        data = response.text.split( ' ' )

        for i in data:

            j += 1
            lst = i.split( ',' )

            if j == 1:
                self.df[ '日期' ] = lst

            elif j == 2:
                self.df[ '開盤' ] = [ float( i ) for i in lst ]

            elif j == 3:
                self.df[ '最高' ] = [ float( i ) for i in lst ]

            elif j == 4:
                self.df[ '最低' ] = [ float( i ) for i in lst ]

            elif j == 5:
                self.df[ '收盤' ] = [ float( i ) for i in lst ]

            elif j == 6:
                self.df[ '成交量' ] = [ float( i ) for i in lst ]

    def get_technical_indicator_dataframe(self):

        C = np.array( self.df[ '收盤' ], dtype = float, ndmin = 1 )
        H = np.array( self.df[ '最高' ], dtype = float, ndmin = 1 )
        L = np.array( self.df[ '最低' ], dtype = float, ndmin = 1 )
        V = np.array( self.df[ '成交量' ], dtype = float, ndmin = 1 )

        self.df[ 'MA03' ] = talib.SMA( C, 3 )

        self.df[ 'MA05' ] = talib.SMA( C, 5 )

        self.df[ 'MA10' ] = talib.SMA( C, 10 )

        self.df[ 'MA20' ] = talib.SMA( C, 15 )

        self.df[ 'MA30' ] = talib.SMA( C, 30 )

        self.df[ 'MA45' ] = talib.SMA( C, 45 )

        self.df[ 'MA60' ] = talib.SMA( C, 60 )

        self.df[ 'MA120' ] = talib.SMA( C, 120 )

        self.df[ 'RSI 12' ] = talib.RSI( C, timeperiod = 12 )

        # ------ MACD Begin. ----------------------------
        # 使用MACD需要设置长短均线和macd平均线的参数
        SHORTPERIOD = 12
        LONGPERIOD = 26
        SMOOTHPERIOD = 9
        # 用Talib计算MACD取值，得到三个时间序列数组，分别为macd,signal 和 hist
        DIF = (H + L + 2 * C) / 4
        self.df[ 'MACD DIF' ], self.df[ 'DEM' ], self.df[ 'OSC' ] = talib.MACD( DIF, SHORTPERIOD, LONGPERIOD, SMOOTHPERIOD )
        # ------ MACD End. ------------------------------

        # -------- MFI Begin. ---------------------------
        self.df[ 'MFI(6)' ] = talib.MFI( H, L, C, V, timeperiod = 6 )
        self.df[ 'MFI(14)' ] = talib.MFI( H, L, C, V, timeperiod = 14 )
        # -------- MFI End. -----------------------------

        # -------- Williams %R Begin. ------------------------
        self.df[ 'WILLR 9' ] = talib.WILLR( H, L, C, timeperiod = 9 )
        self.df[ 'WILLR 18' ] = talib.WILLR( H, L, C, timeperiod = 18 )
        self.df[ 'WILLR 56' ] = talib.WILLR( H, L, C, timeperiod = 56 )
        # -------- Williams %R End. --------------------------

        # -------- Average Directional Movement Index Begin . --------
        self.df[ 'PLUS_DI' ] = talib.PLUS_DI( H, L, C, timeperiod = 14 )
        self.df[ 'MINUS_DI' ] = talib.MINUS_DI( H, L, C, timeperiod = 14 )
        self.df[ 'DX' ] = talib.DX( H, L, C, timeperiod = 14 )
        self.df[ 'ADX' ] = talib.ADX( H, L, C, timeperiod = 14 )
        # ------- Average Directional Movement Index End . --------

        # -------- Bollinger Bands Begin. --------
        # 布林 是 OK，但倒過來
        self.df[ 'Upperband' ], self.df[ 'Middleband' ], self.df[ 'Dnperband' ] = talib.BBANDS( C, timeperiod = 20, nbdevup = 2,
                                                                                    nbdevdn = 2, matype = 0 )
        self.df[ '%BB' ] = (C - self.df[ 'Dnperband' ]) / (self.df[ 'Upperband' ] - self.df[ 'Dnperband' ])
        self.df[ 'W20' ] = (self.df[ 'Upperband' ] - self.df[ 'Dnperband' ]) / self.df[ 'MA20' ]
        # -------- Bollinger Bands Begin. --------

        # ---------------- 乖離 指標 Begin. ------------------------
        # 乖離 OK, 但比較是倒過來
        # 20 Bias=(C-SMA20)/SMA20
        # 60 Bias=(C-SMA60)/SMA60
        self.df[ '20 Bias' ] = ( C - self.df[ 'MA20' ] ) / self.df[ 'MA20' ]
        self.df[ '60 Bias' ] = ( C - self.df[ 'MA60' ] ) / self.df[ 'MA60' ]
        # ---------------- 乖離 指標 End. ------------------------

        self.df = self.df.iloc[ ::-1 ]

def main( ):

    ti_A = Technical_Indicator( '2330', 'D', 10 )

    print( '初始化{0}', ti_A.df  )

    ti_A.get_technical_indicator_dataframe( )

    print( '查詢股號 {0} {1} {2}'.format( ti_A.number, '日線', ti_A.df ) )
    # ------------------------------------------------------

    ti_W =  Technical_Indicator( '2330', 'W', 30 )

    ti_W.get_technical_indicator_dataframe( )

    print( '{0} {1}'.format( '周線', ti_W.df ) )
    # --------------------------------------------------

if __name__ == '__main__':
    main( )


