import crawl_all_stock_info as CASI
import data_transform as Data_Trans
import DB_Connection as DB_conn
import data_dealing as DD
# import yaml
import datetime
import  pandas as pd
import os
# -*- coding: utf-8 -*-
#coding=gbk
'''以下為全域變數'''
USER_NAME = "apps"
PASSWORD = 'apps'
DNS = 'localhost:1521/ORCL'

def main():
    try:
        test_task=3
        print('現在正在測試task',test_task)
        '''處理每日上市&上櫃資料'''
        print("start from main")

        print("=====start crawl_all_stock_info=====")
        if test_task==1:

            OTC_today_info_list=[] #init today OTC data
            OSC_today_info_list=[] #init today OSC data
            OTC_today_info_list,OSC_today_info_list=CASI.crawl_stock_info()
            # OTC_today_info_list= ['2024/07/12', '281.01', '281.01', '278.12', '278.46', '-2.88', '-1.02', '1.03', '1,054.29', '1.24', '-37', '+9.57', '-10.7', '-38.1', '1,173', '-7.47', '6.3', '-0.14','上櫃']
            # OSC_today_info_list= ['2024/07/12', '23955.67', '24045.73', '23774.22', '23916.93', '-473.1', '-1.94', '1.11', '6,011.28', '2.88', '-417', '+67.4', '-150', '-499', '3,337', '-8.57', '22.5', '+0.6','上市']
            if OTC_today_info_list is not None: #如果今天的資料抓出來是空的 那也不需要建立DB連線，也不需要將今日的資料insert到DB
                try:
                    print("OTC_today_info_list--main=", OTC_today_info_list, ' length of OTC_today_info_list=', len(OTC_today_info_list))

                    print("OSC_today_info_list--main=", OSC_today_info_list, ' length of OSC_today_info_list=', len(OSC_today_info_list))


                    conn=DB_conn.get_DB_connection(USER_NAME,PASSWORD,DNS)
                    # 使用cursor()方法获取操作游标
                    cursor = conn.cursor()
                    print('DB connection & DB cursor 已建立')

                    '''以下要insert 每日OTC,OSC資料到DB'''
                    sql="insert into s_stock_market_txns "\
                    "(TXN_DATE,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,DEAL_PRICE,DIFF_PRICE,DIFF_RATE,"\
                    "AMPLITUDE_RATE, TXN_AMOUNT, TXN_AVG_QTY, FI_AMT, CI_AMT, DLR_AMT, II_TTL_AMT, FINANCING_BALANCE,"\
                    "FINANCING_DIFF, LENDING_BALANCE, LENDING_DIFF, STOCK_TYPE) "\
                    "select :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19 from dual "\
                    "where not exists ( select 1 from s_stock_market_txns t where t.TXN_DATE= :1 and STOCK_TYPE = :19)"
                    # "values"\
                    # "(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19)"
                    '''插入OTC當日資料'''
                    cursor.execute(sql, (OTC_today_info_list[0],OTC_today_info_list[1],OTC_today_info_list[2],OTC_today_info_list[3],OTC_today_info_list[4],
                                         OTC_today_info_list[5],OTC_today_info_list[6],OTC_today_info_list[7],OTC_today_info_list[8],OTC_today_info_list[9],
                                         OTC_today_info_list[10],OTC_today_info_list[11],OTC_today_info_list[12],OTC_today_info_list[13],OTC_today_info_list[14],
                                         OTC_today_info_list[15],OTC_today_info_list[16],OTC_today_info_list[17],OTC_today_info_list[18])
                                   )
                    '''插入OSC當日資料'''
                    cursor.execute(sql, (OSC_today_info_list[0],OSC_today_info_list[1],OSC_today_info_list[2],OSC_today_info_list[3],OSC_today_info_list[4],
                                         OSC_today_info_list[5],OSC_today_info_list[6],OSC_today_info_list[7],OSC_today_info_list[8],OSC_today_info_list[9],
                                         OSC_today_info_list[10],OSC_today_info_list[11],OSC_today_info_list[12],OSC_today_info_list[13],OSC_today_info_list[14],
                                         OSC_today_info_list[15],OSC_today_info_list[16],OSC_today_info_list[17],OSC_today_info_list[18])
                                   )
                    conn.commit()

                except Exception as e:
                    print('Get DB Connection Exception Occurs code no.=', e)
                    if DB_conn.is_connection_alive(conn):
                        print("Exception Occurs,Close the db connection")
                        cursor.close()
                        conn.close()
                    return None
                finally:
                    if DB_conn.is_connection_alive(conn):
                        print("Finally Occurs,Close the db connection")
                        cursor.close()
                        conn.close()
        if test_task==2:

            '''Task 2 需要以程式登入goodinfo且登入帳號'''
            today = datetime.date.today().strftime('%Y%m%d')
            stock_info_dict_df= {}
            '''investment_trust,technical,basic_info'''

            stock_type_list = ['OSC_上市','OTC_上櫃']  # ,'OTC_上櫃'
            # dealing_list = ['basic_info']

            dealing_list = ['investment_trust','technical','basic_info']
            # dealing_list = ['investment_trust']

            stock_info_dict_df= CASI.get_stock_info(stock_type_list,dealing_list)
            # print("stock_info_dict_df=",stock_info_dict_df)

            try:
                conn = DB_conn.get_DB_connection(USER_NAME, PASSWORD, DNS)
                # 使用cursor()方法获取操作游标
                cursor = conn.cursor()
                print('DB connection & DB cursor 已建立')
                # dealing_list = ['外資']

                dealing_list = ['外資','投信','技術面','基本面']
                # dealing_list = ['基本面']

                insert_sql = ""
                for stock_type in stock_type_list:
                    for deal in dealing_list:
                        insert_sql= DB_conn.get_insert_stockinfo_sql(stock_type,deal)
                        print(f"現在正在插入'{stock_type}'的'{deal}'資料 SQL='{insert_sql}'")
                        DD.insert_stock_data_into_table(insert_sql, stock_type, deal, conn, cursor, stock_info_dict_df)

                        # 未使用到的column name
                        '''
                        名稱
                        法人買賣日期
                        '''
                        '''插入外資,投信,技術面,籌碼面當日資料'''






            except Exception as e:
                print('Get DB Connection Exception Occurs code no.=', e)
                if DB_conn.is_connection_alive(conn):
                    print("Exception Occurs,Close the db connection")
                    cursor.close()
                    conn.close()
                return None
            finally:
                if DB_conn.is_connection_alive(conn):
                    print("Finally Occurs,Close the db connection")
                    cursor.close()
                    conn.close()
        if test_task == 3:
            # print('現在正在測試task', test_task)
            today = datetime.date.today().strftime('%Y%m%d')

            URL_content = CASI.visit_Holding_rate_URL()
            print("type of URL_content=",type(URL_content))
            holding_rate_df=CASI.get_inverst_holding_rate(URL_content,today)
            print("holding_rate_df=",type(holding_rate_df))
            insert_sql = DB_conn.get_insert_holding_rate_sql()

            print("holding_rate_df.index=",holding_rate_df.index.tolist(),"len=",len(holding_rate_df.index.tolist()))
            print("holding_rate_df.columns=",holding_rate_df.columns.tolist())

            # for i in range()
            try:
                conn = DB_conn.get_DB_connection(USER_NAME, PASSWORD, DNS)
                # 使用cursor()方法获取操作游标
                cursor = conn.cursor()

                # 代號,投信持股(%),發行張數,成交量
                for index, col in enumerate(holding_rate_df.index.tolist()):
                    cursor.execute(insert_sql, (
                        str(holding_rate_df['代號'].iloc[index]), str(holding_rate_df['投信持股(%)'].iloc[index]),
                        holding_rate_df['發行張數'].iloc[index],None ) )
                # 提交事务
                conn.commit()


            except Exception as e:
                print('Get DB Connection Exception Occurs code no.=', e)
                if DB_conn.is_connection_alive(conn):
                    print("Exception Occurs,Close the db connection")
                    cursor.close()
                    conn.close()
                return None
            finally:
                if DB_conn.is_connection_alive(conn):
                    print("Finally Occurs,Close the db connection")
                    cursor.close()
                    conn.close()


            # Stock_dict=Data_Trans.get_excel_list()
            # OSC_df,OTC_df=Data_Trans.get_OSC_OTC_info()
            # Data_Trans.data_dealing(Stock_dict,OSC_df,OTC_df)
    except Exception as e:
        print('Get DB Connection Exception Occurs code no.=', e)
        return None





if __name__ == '__main__':
    main()  # 或是任何你想執行的函式