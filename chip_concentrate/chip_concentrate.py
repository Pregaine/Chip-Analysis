import pandas as pd
import datetime

class Chip_Concentrate:
    """

        """

    def __init__( self, source_df, day_list, interval_day = 5 ):

        self.df = pd.DataFrame( )

        self.df = source_df

        self.start_day_list = [ ]

        self.end_day_list = [ ]

        self.inter_day = interval_day

        time_obj = [ day_list[ i: i + interval_day ] for i in range( 0, len( day_list ), interval_day ) ]

        for i in time_obj:
            self.start_day_list.append( i[ 0 ] )
            self.end_day_list.append( i[ - 1 ] )

    def sort_source( self ):

        result = pd.DataFrame( )

        for i in range( len( self.start_day_list ) ):
            # 取出含有字串買賣超金額及券商的columns為另一個Dataframe
            # ------------------------------------------------------------------------------

            tmp = self.df[ ( self.df[ '日期' ] >= self.start_day_list[ i ] ) & ( self.df[ '日期' ] <= self.end_day_list[ i ]) ].copy( )

            tmp = tmp.groupby( [ '券商' ] ).sum( ).reset_index( )

            tmp.sort_values( by = '買賣超張數', axis = 0, ascending = False, inplace = True )

            df_buy15 = tmp[ :15 ]

            df_self15 = tmp[ -15: ]

            chip_buy_count = tmp[ tmp[ '買進均價'] > 0 ].drop_duplicates( subset = [ '券商' ], keep = 'first' )[ '券商' ].count( )

            chip_self_count =  tmp[ tmp[ '賣出均價' ] > 0 ].drop_duplicates( subset = [ '券商' ], keep = 'first' )[ '券商' ].count( )

            # chip_self_count = tmp.drop_duplicates( subset = [ '券商', '日期' ], keep = 'first' )[ '券商' ].count( )
            # ------------------------------------------------------------------------------

            list_all_chip = self.df[
                ((self.df[ '買進均價' ] > 0) | (self.df[ '賣出均價' ] > 0)) & (self.df[ '日期' ] >= self.start_day_list[ i ]) & (
                    self.df[ '日期' ] <= self.end_day_list[ i ]) ][ '券商' ]

            list_all_chip_count = len( set( list_all_chip ) )
            # -----------------------------------------------------------------------------

            # -----------------------------------------------------------------------------
            # 計算前15大買進均價
            # 計算前15大賣出均價
            # 計算前15大買超佔股本比
            # 計算前15大賣超佔股本比
            # ------------------------------------------------------------------------------

            theday_obj = self.start_day_list[ i ]
            endday_obj = self.end_day_list[ i ]

            vol = 0
            close = 0
            cnt = 0

            while theday_obj <= endday_obj:

                vol_df = self.df[ self.df[ '日期' ] == theday_obj ][ '成交量' ].reset_index( )

                if not vol_df.empty:

                    vol = vol + vol_df[ '成交量' ].iloc[ 0 ]

                    close_df = self.df[ self.df[ '日期' ] == theday_obj ][ '收盤' ].reset_index( )

                    close = close + close_df[ '收盤' ].iloc[ 0 ]

                    cnt += 1

                theday_obj += datetime.timedelta( days = 1 )

            close = close / cnt

            df_tmp = pd.DataFrame(

                {
                    # '日期': self.start_day_list[ i ].strftime( "%Y%m%d" ) + '~' + self.end_day_list[ i ].strftime( "%Y%m%d" ),
                    '股本' : self.df.iloc[ 0 ][ '股本' ],

                    '收盤' : close,

                    '成交量' : vol,

                    '前15大買進均價': df_buy15[ '買賣超金額' ].sum( ) / df_buy15[ '買賣超股數' ].sum( ),

                    '前15大買超佔股本比': df_buy15[ '買賣超金額' ].sum( ) / self.df.iloc[ 0 ][ '股本' ] * 100,

                    '前15大賣出均價': df_self15[ '買賣超金額' ].sum( ) / df_self15[ '買賣超股數' ].sum( ),

                    '前15大賣超佔股本比': -df_self15[ '買賣超金額' ].sum( ) / self.df.iloc[ 0 ][ '股本' ] * 100,

                    '前15大買超張數': df_buy15[ '買賣超張數' ].sum( ),

                    '前15大賣超張數': -df_self15[ '買賣超張數' ].sum( ),

                    '前15大買賣超張數': df_buy15[ '買賣超張數' ].sum( ) + df_self15[ '買賣超張數' ].sum( ),

                    '當日總卷商買家數': chip_buy_count,

                    '當日總卷商賣家數': chip_self_count,

                    '當日總卷商買賣家數': list_all_chip_count,

                    str( self.inter_day ) + '日集中度': ( df_buy15[ '買賣超張數' ].sum( ) + df_self15[ '買賣超張數' ].sum( ) ) / vol * 100

                  }, index = [ self.start_day_list[ i ].strftime( "%Y%m%d" ) + '~' + self.end_day_list[ i ].strftime( "%Y%m%d" ) ] )

            result = pd.concat( [ result, df_tmp ] )

        # 整組DataFrame根據index翻轉排序

        result = result.iloc[ ::-1 ]
         # -----------------------------------------------------------------------------

        cols = [ '股本', '前15大買超佔股本比', '前15大賣超佔股本比', '前15大買超張數', '前15大賣超張數', '前15大買賣超張數', '前15大買進均價', '前15大賣出均價',
        '當日總卷商買賣家數', '當日總卷商買家數', '當日總卷商賣家數', str( self.inter_day ) + '日集中度', '收盤', '成交量' ]

        result = result.reindex( columns = cols )

        return result

def main( ):
    pass

if __name__ == '__main__':
    main( )
