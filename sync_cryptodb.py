from binance.enums import * 
import pandas as pd 
import numpy as np
from scipy.signal import argrelextrema 
import pandas_ta as ta
from binance.client import AsyncClient , asyncio
import mysql.connector as mysql
import time ,telebot
bot_start = telebot.TeleBot('1986077457:AAH1jmWWmIbvg9id6Y6w78vmN1zuY4CAmLo')
chat_id = '1898046190'
a = time.time()
bot_start.send_message(chat_id, 'crypto bot en marche ')
"################"

"###############"
try :
    mydb = mysql.connect( 
            host = 'lin-3946-3583-mysql-primary.servers.linodedb.net' ,
            user ='linroot' ,
            password = 'GMsyYGPd9qIKQ+vp' ,
            database = 'cryptomarket'
        )
    print('connection successfuled')
    my_cursar = mydb.cursor()
except Exception as e :
    bot_start.send_message(chat_id, f'la connection a la base de donné a echoué a cause{e}')


async def code_script(ticker) : 
    interval1 = KLINE_INTERVAL_1MINUTE
    interval5= KLINE_INTERVAL_5MINUTE
    interval15= KLINE_INTERVAL_15MINUTE
    interval60= KLINE_INTERVAL_1HOUR
    depth1 = ' 2 hour ago UTC'
    depth5 = ' 4 hours ago UTC'
    depth15 = ' 6 hours ago UTC'
    depth60 = ' 48 hours ago UTC'
        
    
    
    Pkey = 'JBcSmKX00aJzU3KSIuFkasCyNNAF7xYMk3DgSJTbljtcCDbLHGTq5HCX7icrJjqG'
    Skey = 'Z3wOdqlmqljejQjMnuBc482IRUJvltkNMBQmqPjjs0eoF77BKTtH0TQLhW0YXoaa'    
    client = await AsyncClient.create(Pkey, Skey)
    #
    #
    #
    '####'## data requests : 
    Cdata_01 = await client.get_historical_klines(ticker,interval1,depth1 )
    df_1 = pd.DataFrame(Cdata_01)
    cdata5 = await  client.get_historical_klines(ticker,interval5,depth5 )
    df_5 = pd.DataFrame(cdata5)
    cdata_15 = await client.get_historical_klines(ticker,interval15,depth15 )
    df_15 = pd.DataFrame(cdata_15)
    cdata_60 = await client.get_historical_klines(ticker,interval60,depth60 )
    df_60 = pd.DataFrame(cdata_60)
    #
    #
    #
    #
    #
    #
    #
    #
    '####' #Manipulation of data :
    '1 min'
    if not df_1.empty: 
        df_1[0] =  pd.to_datetime(df_1[0],unit='ms')
        df_1.columns = ['time','open','high','low','close','volume','IGNORE','Quote_Volume','Trades_Count','BUY_VOL','BUY_VOL_VAL','x']
        df_1 = df_1.set_index('time')
        del df_1['IGNORE']    
        del df_1['BUY_VOL']
        del df_1['BUY_VOL_VAL']
        del df_1['x']
        del df_1[ 'volume']    
        del df_1['Trades_Count']
        df_1 ["open"] = pd.to_numeric(df_1["open"])
        df_1 ["high"] = pd.to_numeric(df_1["high"])
        df_1 ["low"] = pd.to_numeric(df_1["low"])
        df_1 ["close"] = pd.to_numeric(df_1["close"])
        df_1 ['quoteVolume'] = pd.to_numeric(df_1['Quote_Volume'])
        if df_1 ['quoteVolume'][-2] == 0 :
            V_change_1 = 0
        else :  
            V_change_1=(df_1['quoteVolume']/df_1 ["quoteVolume"].shift(1).fillna(0))
            V_change_1 = V_change_1[-1]
        b = argrelextrema(df_1.close.values, np.less_equal, order=1)[0]
        peak_min_01 = df_1.iloc[b]
        a = argrelextrema(df_1.close.values, np.greater_equal, order=1)[0]
        peak_max_01=df_1.iloc[a]


        # le point de corde sur les 1min
        if peak_min_01.iloc[-2 , 3]<= peak_min_01.iloc[-1 , 3] and peak_max_01.iloc[-2,3]<= df_1.close[-1] :
            le_point_de_corde_1 = True
            color = 1
        if peak_min_01.iloc[-2 , 3]<= peak_min_01.iloc[-1 , 3] :
                color = 0
        else : 
            color = -1

        b_bands = ta.bbands(df_1.close, length=21)
        trend_direction_1min = (b_bands['BBM_21_2.0'] -  b_bands['BBM_21_2.0'].shift(2))/df_1.close
        # potentiel win :
        if trend_direction_1min[-1] <= 20 : 
            potentiel_win_1 = ((b_bands['BBM_21_2.0'][-1] - df_1 ["close"][-1])/df_1 ["close"][-1])*100
        else : 
            potentiel_win_1 = ((b_bands['BBM_21_2.0'][-1] - df_1 ["close"][-1])/df_1 ["close"][-1])*100
        # potentiel loss : 
        potentiel_loss_1_1 = ((df_1 ["close"][-1] - peak_min_01.iloc[-1 , 3])/df_1 ["close"][-1])*100 
        potentiel_loss_1_2 = ((df_1 ["close"][-1] - peak_min_01.iloc[-2 , 3])/df_1 ["close"][-1])*100 
    else : 
        pass
    '5 min'
    if not df_5.empty: 
        df_5[0] =  pd.to_datetime(df_5[0],unit='ms')
        df_5.columns = ['time','open','high','low','close','volume','IGNORE','Quote_Volume','Trades_Count','BUY_VOL','BUY_VOL_VAL','x']
        df_5 = df_5.set_index('time')
        del df_5['IGNORE']    
        del df_5['BUY_VOL']
        del df_5['BUY_VOL_VAL']
        del df_5['x']
        del df_5[ 'volume']    
        del df_5['Trades_Count']
        df_5 ["open"] = pd.to_numeric(df_5["open"])
        df_5 ["high"] = pd.to_numeric(df_5["high"])
        df_5 ["low"] = pd.to_numeric(df_5["low"])
        df_5 ["close"] = pd.to_numeric(df_5["close"])
        df_5 ['quoteVolume'] = pd.to_numeric(df_5['Quote_Volume'])
        if df_5 ['quoteVolume'][-2] == 0 :
            V_change_5 = 0
        else :  
            V_change_5=(df_5['quoteVolume']/df_5 ["quoteVolume"].shift(1).fillna(0))
            V_change_5 = V_change_5[-1]
        b = argrelextrema(df_5.close.values , np.less_equal, order=1)[0]
        peak_min_5 = df_5.iloc[b]
        a = argrelextrema(df_5.close.values , np.greater_equal, order=1)[0]
        peak_max_5=df_5.iloc[a]


        # le point de corde sur les 1min
        if peak_min_5.iloc[-2 , 3]<= peak_min_5.iloc[-1 , 3] and peak_max_5.iloc[-2,3]<= df_5.close[-1] :
            le_point_de_corde_1 = True
            color_5 = 1
        if peak_min_5.iloc[-2 , 3]<= peak_min_5.iloc[-1 , 3]:
            color_5 = 0
        else : 
            color_5 = -1

        bbands_5 = ta.bbands(df_5.close, length=21)
        trend_direction_5min = (bbands_5['BBM_21_2.0'] -  bbands_5['BBM_21_2.0'].shift(2))/df_5.close
        # potentiel win :
        if trend_direction_5min[-1] <= 20 : 
            potentiel_win_5 = ((bbands_5['BBM_21_2.0'][-1] - df_5 ["close"][-1])/df_5 ["close"][-1])*100
        else : 
            potentiel_win_5 = ((bbands_5['BBM_21_2.0'][-1] - df_5 ["close"][-1])/df_5 ["close"][-1])*100
        # potentiel loss : 
        potentiel_loss_5_1 = ((df_5 ["close"][-1] - peak_min_5.iloc[-1 , 3])/df_5 ["close"][-1])*100 
        potentiel_loss_5_2 = ((df_5 ["close"][-1] - peak_min_5.iloc[-2 , 3])/df_5 ["close"][-1])*100 
    else : 
        pass  
    '15 min'
    if not df_15.empty: 
        df_15[0] =  pd.to_datetime(df_15[0],unit='ms')
        df_15.columns = ['time','open','high','low','close','volume','IGNORE','Quote_Volume','Trades_Count','BUY_VOL','BUY_VOL_VAL','x']
        df_15 = df_15.set_index('time')
        del df_15['IGNORE']    
        del df_15['BUY_VOL']
        del df_15['BUY_VOL_VAL']
        del df_15['x']
        del df_15[ 'volume']    
        del df_15['Trades_Count']
        df_15 ["open"] = pd.to_numeric(df_15["open"])
        df_15 ["high"] = pd.to_numeric(df_15["high"])
        df_15 ["low"] = pd.to_numeric(df_15["low"])
        df_15 ["close"] = pd.to_numeric(df_15["close"])
        df_15 ['quoteVolume'] = pd.to_numeric(df_15['Quote_Volume'])
        if df_15 ['quoteVolume'][-2] == 0 :
            V_change_15 = 0
        else :  
            V_change_15 =(df_15['quoteVolume']/df_15 ["quoteVolume"].shift(1).fillna(0))
            V_change_15 = V_change_15[-1]
        b = argrelextrema(df_15.close.values, np.less_equal, order=1)[0]
        peak_min_15 = df_15.iloc[b]
        a = argrelextrema(df_15.close.values, np.greater_equal, order=1)[0]
        peak_max_15=df_15.iloc[a]


        # le point de corde sur les 1min
        if peak_min_15.iloc[-2 , 3]<= peak_min_15.iloc[-1 , 3] and peak_max_15.iloc[-2,3]<= df_15.close[-1] :
            le_point_de_corde_1 = True
            color_15 =1
        if peak_min_15.iloc[-2 , 3]<= peak_min_15.iloc[-1 , 3] :
            color_15 =0
        else : 
            color_15 = -1

        b_bands_15 = ta.bbands(df_15.close, length=21)
        trend_direction_5min = (b_bands_15['BBM_21_2.0'] -  b_bands_15['BBM_21_2.0'].shift(2))/df_15.close
        # potentiel win :
        if trend_direction_5min[-1] <= 20 : 
            potentiel_win_15 = ((b_bands_15['BBM_21_2.0'][-1] - df_15 ["close"][-1])/df_15 ["close"][-1])*100
        else : 
            potentiel_win_15 = ((b_bands_15['BBM_21_2.0'][-1] - df_15 ["close"][-1])/df_15 ["close"][-1])*100
        # potentiel loss : 
        potentiel_loss_15_1 = ((df_15 ["close"][-1] - peak_min_15.iloc[-1 , 3])/df_15 ["close"][-1])*100 
        potentiel_loss_15_2 = ((df_15 ["close"][-1] - peak_min_15.iloc[-2 , 3])/df_15 ["close"][-1])*100 
    else : 
        pass
    '60 min'
    if not df_60.empty: 
        df_60[0] =  pd.to_datetime(df_60[0],unit='ms')
        df_60.columns = ['time','open','high','low','close','volume','IGNORE','Quote_Volume','Trades_Count','BUY_VOL','BUY_VOL_VAL','x']
        df_60 = df_60.set_index('time')
        del df_60['IGNORE']    
        del df_60['BUY_VOL']
        del df_60['BUY_VOL_VAL']
        del df_60['x']
        del df_60[ 'volume']    
        del df_60['Trades_Count']
        df_60 ["open"] = pd.to_numeric(df_60["open"])
        df_60 ["high"] = pd.to_numeric(df_60["high"])
        df_60 ["low"] = pd.to_numeric(df_60["low"])
        df_60 ["close"] = pd.to_numeric(df_60["close"])
        df_60 ['quoteVolume'] = pd.to_numeric(df_60['Quote_Volume'])
        if df_60 ['quoteVolume'][-2] == 0 :
            V_change_60 = 0
        else :  
            V_change_60 = df_60['vchange15min'] =(df_60['quoteVolume']/df_60 ["quoteVolume"].shift(1).fillna(0))
            V_change_60 = V_change_60[-1]      
        b = argrelextrema(df_60.close.values, np.less_equal, order=1)[0]
        peak_min_60 = df_60.iloc[b]
        a = argrelextrema(df_60.close.values, np.greater_equal, order=1)[0]
        peak_max_60=df_60.iloc[a]


            # le point de corde sur les 1min
        if peak_min_60.iloc[-2 , 3]<= peak_min_60.iloc[-1 , 3] and peak_max_60.iloc[-2,3]<= df_60.close[-1] :
            le_point_de_corde_1 = True
            color_60 =  1
        if peak_min_60.iloc[-2 , 3]<= peak_min_60.iloc[-1 , 3] :
            color_60 = 0
        else : 
            color_60 = -1

        b_bands_60 = ta.bbands(df_60.close, length=21)
        trend_direction_60min = (b_bands_60['BBM_21_2.0'] -  b_bands_60['BBM_21_2.0'].shift(2))/df_60.close
        # potentiel win :
        if trend_direction_60min[-1] <= 20 : 
            potentiel_win_60 = ((b_bands_60['BBM_21_2.0'][-1] - df_60 ["close"][-1])/df_60 ["close"][-1])*100
        else : 
            potentiel_win_60 = ((b_bands_60['BBM_21_2.0'][-1] - df_60 ["close"][-1])/df_60 ["close"][-1])*100
        # potentiel loss : 
        potentiel_loss_60_1 = ((df_60 ["close"][-1] - peak_min_60.iloc[-1 , 3])/df_60 ["close"][-1])*100 
        potentiel_loss_60_2 = ((df_60 ["close"][-1] - peak_min_60.iloc[-2 , 3])/df_60 ["close"][-1])*100 
    else : 
        pass
    '#########' # database Update
   
    
    try :
        sql = ("UPDATE marketcrypto SET P_win_1 =%s , P_loss_1_1 =%s  , P_loss_1_2 =%s , V_Change_1 =%s , Color_1=%s  , P_win_5 =%s , P_loss_5_1 =%s , P_loss_5_2 =%s , V_Change_5=%s , Color_5=%s  ,P_win_15 =%s  , P_loss_15_1=%s , P_loss_15_2 =%s ,V_Change_15=%s , Color_15=%s  ,P_win_60 =%s , P_loss_60_1=%s  , P_loss_60_2=%s ,V_Change_60 =%s, Color_60=%s  WHERE Ticker = '{}' ".format(ticker))
        variable = (f'{potentiel_win_1}',f'{potentiel_loss_1_1}',f'{potentiel_loss_1_2}',f'{V_change_1}',f'{color}',f'{potentiel_win_5}',f'{potentiel_loss_5_1}',f'{potentiel_loss_5_2}',f'{V_change_5}',f'{color_5}',f'{potentiel_win_15}',f'{potentiel_loss_15_1}',f'{potentiel_loss_15_2}',f'{V_change_15}',f'{color_15}',f'{potentiel_win_60}',f'{potentiel_loss_60_1}',f'{potentiel_loss_60_2}',f'{V_change_60}',f'{color_60}')
        my_cursar.execute(sql , variable)
        mydb.commit()    
    except Exception as e :
        bot_start.send_message(chat_id, f'un probleme sur le titre {ticker} probalement a cause du {e}')
        pass
    await client.close_connection()
    print (f'done for {ticker}') 
async def main(tikcers) : 
    all = await asyncio.gather(*[code_script(ticker) for ticker in tikcers])
                 

tickers = ['1INCHUSDT','AAVEUSDT','ACAUSDT','ACHUSDT','ACMUSDT','ADAUSDT','ADXUSDT','AGLDUSDT','AIONUSDT','AKROUSDT','ALCXUSDT','ALGOUSDT','ALICEUSDT','ALPACAUSDT','ALPHAUSDT','ALPINEUSDT','AMPUSDT','AMPUSDT','ANCUSDT','ANKRUSDT','ANTUSDT','APEUSDT','ARDRUSDT','ARPAUSDT','ARUSDT','ASRUSDT','ASTRUSDT','ATAUSDT','ATMUSDT','ATOMUSDT','AUCTIONUSDT','AUDIOUSDT','AUTOUSDT','AVAUSDT','AVAXUSDT','AXSUSDT','BADGERUSDT','BAKEUSDT','BALUSDT','BANDUSDT','BARUSDT','BATUSDT','BCHUSDT','BEAMUSDT','BELUSDT','BETAUSDT','BICOUSDT','BIFIUSDT','BLZUSDT','BNBUSDT','BNXUSDT','BNTUSDT','BONDUSDT','BSWUSDT','BTCSTUSDT','BTCUSDT','BTGUSDT','BTSUSDT','BTTCUSDT','BURGERUSDT','C98USDT','CAKEUSDT','CELOUSDT','CELRUSDT','CFXUSDT','CHESSUSDT','CHRUSDT','CHZUSDT','CITYUSDT','CKBUSDT','CLVUSDT','COCOSUSDT','COMPUSDT','COSUSDT','COTIUSDT','CRVUSDT','CTKUSDT','CTSIUSDT','CTXCUSDT','CVCUSDT','CVPUSDT','CVXUSDT','DARUSDT','DASHUSDT','DATAUSDT','DCRUSDT','DEGOUSDT','DENTUSDT','DEXEUSDT','DFUSDT','DGBUSDT','DIAUSDT','DNTUSDT','DOCKUSDT','DODOUSDT','DOGEUSDT','DOTUSDT','DREPUSDT','DUSKUSDT','DYDXUSDT','EGLDUSDT','ELFUSDT','ENJUSDT','ENSUSDT','EOSUSDT','ERNUSDT','ETCUSDT','ETHUSDT','FARMUSDT','FETUSDT','FIDAUSDT' ]  
asyncio.run(main(tickers))
b = time.time()

bot_start.send_message(chat_id, f'Le Temps du tour  : {(b-a)} S')

print(b-a)
