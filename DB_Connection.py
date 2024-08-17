import cx_Oracle

# 连接数据库，下面括号里内容根据自己实际情况填写
# def get_DB_connection(user_name,password,db_dns):
def get_DB_connection(user_name,password,db_dns):
    try:
        # conn = cx_Oracle.connect('apps', 'apps', 'localhost:1521/ORCL', mode=cx_Oracle.SYSDBA)
        print('user_name=',user_name,'\n'
              'password=', password, '\n'
              'db_dns=', db_dns, '\n'
              )
        conn = cx_Oracle.connect(user_name, password, db_dns, mode=cx_Oracle.SYSDBA) #建立連線
        print('connect_result=', conn)

        # 使用cursor()方法获取操作游标
        # cursor = conn.cursor()

        # # 使用execute方法执行SQL语句
        # result = cursor.execute(
        # '''Select stock_no from S_STOCK_DAILY_TXNS t where t.datecode = to_char(sysdate-2,'yyyymmdd') and stock_no not like '0%' and rownum < 2 ''')

        # 使用fetchone()方法获取一条数据
        # data=cursor.fetchone()

        # 获取部分数据，8条
        # many_data=cursor.fetchmany(8)

        # 获取所有数据
        # all_data = cursor.fetchall()

        # for row in all_data:
        #     print(row)
        # output = pd.DataFrame(all_data, columns=['Stock'])
        return conn
    except Exception as e:
        print('Get DB Connection Exception Occurs code no.=',e)
        if is_connection_alive(conn):
            print("Exception Occurs,Close the db connection")
            # cursor.close()
            conn.close()
        return None


def is_connection_alive(connection): #確定DB連線是否還存活著
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT 1 FROM dual")  # 這是一個輕量級的測試查詢
        cursor.close()
        return True
    except cx_Oracle.DatabaseError:
        return False

def get_insert_holding_rate_sql():
    return  """
            INSERT INTO s_stock_chip_txns_temp(Stock_no,Hold_Rate,Ttl_Qty,Qty)
            VALUES
                                              (:1,:2,:3,:4)"""



def get_insert_stockinfo_sql(stock_type,deal):

    if deal == "外資":
        return  """INSERT INTO s_stock_chip_txns(datecode,stock_no, buy_qty, sell_qty, diff_qty,
                                          buy_amount, sell_amount, diff_amount,buy_ttl_rate, sell_ttl_rate,
                                          diff_ttl_rate, buy_rate, sell_rate, diff_rate,cont_day,
                                          cont_qty, cont_remark, cont_rate, cont_ttl_rate, hold_qty,
                                          hold_rate,data_type)
            VALUES
                                         (:1,:2,:3,:4,:5,
                                          :6,:7,:8,:9,:10,
                                          :11,:12,:13,:14,:15, 
                                          :16,:17,:18,:19,:20,
                                          :21 ,:22          )"""
    elif deal == "投信":
        return """INSERT INTO s_stock_chip_txns(datecode,stock_no, buy_qty, sell_qty, diff_qty,
                                          buy_amount, sell_amount, diff_amount,buy_ttl_rate, sell_ttl_rate,
                                          diff_ttl_rate, buy_rate, sell_rate, diff_rate,cont_day,
                                          cont_qty, cont_remark, cont_rate, cont_ttl_rate, hold_qty,
                                          hold_rate,data_type)
            VALUES
                                         (:1,:2,:3,:4,:5,
                                          :6,:7,:8,:9,:10,
                                          :11,:12,:13,:14,:15, 
                                          :16,:17,:18,:19,:20,
                                          :21 ,:22          )"""
    elif deal == "基本面":
        return """INSERT INTO s_stock_daily_txns(DATECODE,STOCK_NO,DEAL_PRICE,DIFF_PRICE,DIFF_RATE,
                                                OPEN_PRICE,HIGH_PRICE,LOW_PRICE,AMPLITUDE_RATE,
                                                TXN_QTY,TXN_AMOUNT,PER,
                                                FINANCING_DIFF,LENDING_SELL_DIFF,LENDING_SELL_BAL,
                                                LATEST_SEASON_REV_YOY,LATEST_SEASON_GPM_YOY,LATEST_MONTH_REV_YOY,
                                                LATEST_SEASON_ACC_EPS,LATEST_4Q_EPS,LATEST_YEAR_EPS,
                                                CASH_DIVIDEND,STOCK_DIVIDEND
                                                )
                          VALUES
                                               (:1,:2,:3,:4,:5,
                                               :6,:7,:8,:9,
                                               :10,:11,:12,
                                                 :13,:14,:15,
                                                 :16,:17,:18,
                                                 :19,:20,:21,
                                                 :22,:23)"""
    elif deal == "技術面":
        return """INSERT INTO s_stock_tech_txns(datecode,stock_no,deal_price,diff_price,diff_rate,txn_qty,txn_amount,per,
                                                ma5,ma10,ma20,ma60,
                                                rsi6_d,rsi12_d,rsi6_w,rsi12_w,
                                                k_d,d_d,k_w,d_w,k_m,d_m,
                                                bias_5,bias_10,bias_20,bias_60,
                                                osc_d,osc_w,osc_m,osc_s ,data_type)
                          VALUES
                                               (:1,:2,:3,:4,:5,:6,:7,:8,
                                                 :9,:10,:11,:12,
                                                 :13,:14,:15,:16,
                                                 :17,:18,:19,:20,:21,:22,
                                                 :23,:24,:25,:26,
                                                 :27,:28,:29,:30,:31 )"""


