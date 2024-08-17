import datetime
import os

import pandas as pd
import DB_Connection as DB_conn
from openpyxl import load_workbook
# -*- coding: utf-8 -*-
#coding=gbk
def dealing_excel_into_dataframe(root_path,stock_type,category,today,stock_dataframe_dict):
    try:
        print(f"dealing_excel_into_dataframe, stock_type= '{stock_type}' category='{category}'  today='{today}' root_path = '{root_path}' ")
        # if stock_type == 'OSC_上市':
            #investment_trust,technical,basic_info

            # if category == 'investment_trust':

        stock_dataframe_dict[stock_type][category] = dealing_excel_by_category(root_path,stock_type,category,today)

            # elif category == 'technical':
            #
            # elif category == 'basic_info':

        return stock_dataframe_dict
        # elif stock_type == 'OTC_上櫃':
        #     print("stock_type=",stock_type)
    except Exception as e:
        print("An error occurred from dealing_excel_into_datafram:", e)
        return None
def dealing_excel_by_category(root_path,stock_type,category,today):
    # if stock_type=='OSC_上市' :
    folder_path = os.path.join('D:\PycharmProjects\milktea_project\Excel_Data',stock_type,'mapping_result')
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


    result_path = os.path.join('D:\PycharmProjects\milktea_project\Excel_Data',stock_type,'mapping_result',today)
    # elif stock_type=='OTC_上櫃' :
    #     result_path = os.path.join('D:\PycharmProjects\milktea_project\Excel_Data\OTC_上櫃\mapping_result',today)

    if not os.path.exists(result_path):
        os.makedirs(result_path)

    #如果是籌碼資料的話分先處理外資，在處理投信
    investment_trust_file_dict_foreign={
        '法人買賣_三大_法人買賣張數(日)':['代號','名稱','法人買賣日期','外資買進張數','外資賣出張數','外資買賣超張數'],
        '法人買賣_三大_法人買賣金額(百萬元)(日)': ['外資買進金額','外資賣出金額','外資買賣超金額'],
        '法人買賣_三大_法人買賣佔發行張數(日)': ['外資買進佔發行張數','外資賣出佔發行張數','外資買賣超佔發行張數'],
        '法人買賣_三大_法人買賣佔成交比重(日)': ['外資買進佔成交','外資賣出佔成交','外資買賣超佔成交'],
        '法人買賣_三大_法人連買連賣統計(日)': ['外資連續買賣日數','外資連續買賣張數','外資連續買賣佔成交(%)','外資連續買賣佔發行量(%)'],
        '法人買賣_三大_法人連買連賣轉折點(日)': ['外資連日買賣轉折'],
        '法人買賣_三大_法人持股狀況(日)': ['外資持有(千張)','外資持股(%)'],
    }

    investment_trust_file_dict_trust = {
        '法人買賣_三大_法人買賣張數(日)': ['代號', '名稱', '法人買賣日期', '投信買進張數', '投信賣出張數', '投信買賣超張數'],
        '法人買賣_三大_法人買賣金額(百萬元)(日)': ['投信買進金額', '投信賣出金額', '投信買賣超金額'],
        '法人買賣_三大_法人買賣佔發行張數(日)': ['投信買進佔發行張數', '投信賣出佔發行張數', '投信買賣超佔發行張數'],
        '法人買賣_三大_法人買賣佔成交比重(日)': ['投信買進佔成交', '投信賣出佔成交', '投信買賣超佔成交'],
        '法人買賣_三大_法人連買連賣統計(日)': ['投信連續買賣日數', '投信連續買賣張數', '投信連續買賣佔成交(%)', '投信連續買賣佔發行量(%)'],
        '法人買賣_三大_法人連買連賣轉折點(日)': ['投信連日買賣轉折'],
        '法人買賣_三大_法人持股狀況(日)': ['投信持有(千張)', '投信持股(%)'],
    }

    technical_file_dict_trust={
        '交易狀況_日' : ['代號','名稱','成交','漲跌價','漲跌幅','成交張數','成交額(百萬)','PER'],
       '移動均線_目前位置1(元)' :['5日均線','10日均線','20日均線','60日均線'],
        'RSI_未還原權值':['RSI6(日)','RSI12(日)','RSI6(週)','RSI12(週)'],
        'KD指標_日':['K值(日)','D值(日)'],
        'KD指標_日_週_月':['K值(週)','D值(週)','K值(月)','D值(月)'],
        '移動均線_乖離率1(%)':['5日均價乖離率','10日均價乖離率','20日均價乖離率','60日均價乖離率'],
        'MACD_日_週_月':['OSC(日)','OSC(週)','OSC(月)'],
        'MACD_季_年':['OSC(季)']
    }





    basic_file_dict_trust={
        '交易狀況_日' : ['代號','名稱','成交','漲跌價','漲跌幅','開盤','最高','最低','振幅(%)','成交張數','成交額(百萬)','PER'],
       '融資融券_資券增減統計(日)' :['融資增減'],
        '融資融券_借券增減統計(日)':['借券賣出增減',	'借券賣出餘額'],
        '季獲利能力_獲利能力 (年增減統計)':['營收成長(%)','毛利成長(%)','EPS(元)'],
        '營收狀況_近N個月一覽_年增率':['24M06年增率'],
        '近四季獲利能力_獲利能力 (年增減統計)':['EPS(元)'],
        '年獲利能力_獲利能力':['EPS(元)'],
        '股利政策發放年度_股利分配資料 (以最後成交價統計)':['現金股利','股票股利']
    }


    result_dict = {}
    if category=='investment_trust':
        #外資
        excel_list_name=['外資','投信']
        investment_trust_file_dict={}


        for k in excel_list_name:

            if k =='外資':
                investment_trust_file_dict=investment_trust_file_dict_foreign
            elif k =='投信':
                investment_trust_file_dict=investment_trust_file_dict_trust

            key_list = list(investment_trust_file_dict.keys())
            series_list = []
            print(f"Dealing with '{k}' right now...")
            investment_trust_dataframe = pd.DataFrame()

            for i in range(len(key_list)):

                file_path = os.path.join(root_path,key_list[i]+'.xls')

                print("file_path="+file_path)
                df_temp = pd.read_html(file_path)#讀取檔案，發現其實他是HTML檔 用notepad++打開就能知道
                # df = pd.read_excel(file_path, engine='xlrd')
                column_list=df_temp[0].columns.tolist()
                row_list = df_temp[0].index.tolist()
                for j in column_list:
                    if df_temp[0][j][0] in investment_trust_file_dict[key_list[i]]:

                            series_list.append(df_temp[0][j])

            investment_trust_dataframe = pd.concat(series_list, axis=1)
            column_of_df = list(investment_trust_dataframe.iloc[0,0:])
            # print('column_of_df=',column_of_df)
            investment_trust_dataframe.set_axis(column_of_df, axis=1, inplace=True)
            investment_trust_dataframe=investment_trust_dataframe.iloc[1:,:]
            # print('investment_trust_dataframe.columns=',investment_trust_dataframe.columns)
            # print('investment_trust_dataframe.columns=',investment_trust_dataframe.index)

            # investment_trust_dataframe.to_excel(os.path.join(result_path,k+'.xlsx'),index=False) # 2024 0804

            result_dict[k]=investment_trust_dataframe
        return result_dict
    elif category=='technical':
        # 技術面
        excel_list_name = ['技術面']
        investment_trust_file_dict = {}

        for k in excel_list_name:

            investment_trust_file_dict = technical_file_dict_trust

            key_list = list(investment_trust_file_dict.keys())
            series_list = []
            print(f"Dealing with '{k}' right now...")
            investment_trust_dataframe = pd.DataFrame()

            for i in range(len(key_list)):

                file_path = os.path.join(root_path, key_list[i] + '.xls')

                print("file_path=" + file_path)
                df_temp = pd.read_html(file_path)  # 讀取檔案，發現其實他是HTML檔 用notepad++打開就能知道
                # df = pd.read_excel(file_path, engine='xlrd')
                column_list = df_temp[0].columns.tolist()
                row_list = df_temp[0].index.tolist()
                for j in column_list:
                    if df_temp[0][j][0] in investment_trust_file_dict[key_list[i]]:
                        series_list.append(df_temp[0][j])

            investment_trust_dataframe = pd.concat(series_list, axis=1)
            column_of_df = list(investment_trust_dataframe.iloc[0, 0:])
            # print('column_of_df=',column_of_df)
            investment_trust_dataframe.set_axis(column_of_df, axis=1, inplace=True)
            investment_trust_dataframe = investment_trust_dataframe.iloc[1:, :]
            # print('investment_trust_dataframe.columns=',investment_trust_dataframe.columns)
            # print('investment_trust_dataframe.columns=',investment_trust_dataframe.index)

            # investment_trust_dataframe.to_excel(os.path.join(result_path, k + '.xlsx'),index=False) # 2024 0804
            result_dict[k] = investment_trust_dataframe
        return result_dict
    elif category=='basic_info':
        # 技術面
        excel_list_name = ['基本面']
        investment_trust_file_dict = {}

        for k in excel_list_name:

            investment_trust_file_dict = basic_file_dict_trust

            key_list = list(investment_trust_file_dict.keys())
            series_list = []
            print(f"Dealing with '{k}' right now...")
            investment_trust_dataframe = pd.DataFrame()

            for i in range(len(key_list)):

                file_path = os.path.join(root_path, key_list[i] + '.xls')

                print("file_path=" + file_path)
                df_temp = pd.read_html(file_path)  # 讀取檔案，發現其實他是HTML檔 用notepad++打開就能知道
                # df = pd.read_excel(file_path, engine='xlrd')
                column_list = df_temp[0].columns.tolist()
                row_list = df_temp[0].index.tolist()
                for j in column_list:
                    if df_temp[0][j][0] in investment_trust_file_dict[key_list[i]]:
                        if df_temp[0][j][0]=='EPS(元)': #'''例外處理 EPS(元)會重複欄位'''
                            df_temp[0][j][0]=key_list[i]+'_'+df_temp[0][j][0]

                        series_list.append(df_temp[0][j])


            investment_trust_dataframe = pd.concat(series_list, axis=1)
            column_of_df = list(investment_trust_dataframe.iloc[0, 0:])
            # print('column_of_df=',column_of_df)
            investment_trust_dataframe.set_axis(column_of_df, axis=1, inplace=True)
            investment_trust_dataframe = investment_trust_dataframe.iloc[1:, :]
            # print('investment_trust_dataframe.columns=',investment_trust_dataframe.columns)
            # print('investment_trust_dataframe.columns=',investment_trust_dataframe.index)

            investment_trust_dataframe.to_excel(os.path.join(result_path, k + '.xlsx'),index=False) # 2024 0804
            result_dict[k] = investment_trust_dataframe
        return result_dict
def insert_stock_data_into_table(sql,stock_type,deal,conn,cursor,stock_info_dict_df):
    try:
        if deal == "外資":

            print(f" try to insert stock_type '{deal}'data into DB...")

            # dealing_list = ['investment_trust','technical','basic_info']
            temp_dict = stock_info_dict_df[stock_type]['investment_trust'][deal]

            for i in list(temp_dict.keys()):
                temp_dict[i] = temp_dict[i].where(
                    pd.notnull(temp_dict[i]), None)

            # print("temp_dict.keys()=",temp_dict.keys())
            # print("stock_info_dict_df[stock_type]['investment_trust'].keys=",stock_info_dict_df[stock_type]['investment_trust'].keys())
            for i in range(len(temp_dict['代號'])):
                # print(f"index= '{i}'")
                cursor.execute(sql, (
                    str(temp_dict['法人買賣日期'].iloc[i]), str(temp_dict['代號'].iloc[i]), temp_dict['外資買進張數'].iloc[i],
                    temp_dict['外資賣出張數'].iloc[i], temp_dict['外資買賣超張數'].iloc[i], temp_dict['外資買進金額'].iloc[i],
                    temp_dict['外資賣出金額'].iloc[i], temp_dict['外資買賣超金額'].iloc[i], temp_dict['外資買進佔發行張數'].iloc[i],
                    temp_dict['外資賣出佔發行張數'].iloc[i], temp_dict['外資買賣超佔發行張數'].iloc[i],
                    temp_dict['外資買進佔成交'].iloc[i], temp_dict['外資賣出佔成交'].iloc[i], temp_dict['外資買賣超佔成交'].iloc[i],
                    temp_dict['外資連續買賣日數'].iloc[i], temp_dict['外資連續買賣張數'].iloc[i],
                    temp_dict['外資連日買賣轉折'].iloc[i], temp_dict['外資連續買賣佔成交(%)'].iloc[i],
                    temp_dict['外資連續買賣佔發行量(%)'].iloc[i],
                    temp_dict['外資持有(千張)'].iloc[i], temp_dict['外資持股(%)'].iloc[i], '外資'))

            # 提交事务
            conn.commit()
        elif deal == "投信":
            print(f" try to insert stock_type '{deal}'data into DB...")

            temp_dict = stock_info_dict_df[stock_type]['investment_trust'][deal]

            for i in list(temp_dict.keys()):
                temp_dict[i] = temp_dict[i].where(pd.notnull(temp_dict[i]), None)

            for i in range(len(temp_dict['代號'])):
                # print(f"index= '{i}'")
                cursor.execute(sql, (
                    str(temp_dict['法人買賣日期'].iloc[i]), str(temp_dict['代號'].iloc[i]), temp_dict['投信買進張數'].iloc[i],
                    temp_dict['投信賣出張數'].iloc[i], temp_dict['投信買賣超張數'].iloc[i], temp_dict['投信買進金額'].iloc[i],
                    temp_dict['投信賣出金額'].iloc[i], temp_dict['投信買賣超金額'].iloc[i], temp_dict['投信買進佔發行張數'].iloc[i],
                    temp_dict['投信賣出佔發行張數'].iloc[i], temp_dict['投信買賣超佔發行張數'].iloc[i],
                    temp_dict['投信買進佔成交'].iloc[i], temp_dict['投信賣出佔成交'].iloc[i], temp_dict['投信買賣超佔成交'].iloc[i],
                    temp_dict['投信連續買賣日數'].iloc[i], temp_dict['投信連續買賣張數'].iloc[i],
                    temp_dict['投信連日買賣轉折'].iloc[i], temp_dict['投信連續買賣佔成交(%)'].iloc[i],
                    temp_dict['投信連續買賣佔發行量(%)'].iloc[i],
                    None, None, '投信'))
            conn.commit()
        elif deal == "技術面":
            print(f" try to insert stock_type '{deal}'data into DB...")

            temp_dict = stock_info_dict_df[stock_type]['technical'][deal]

            for i in list(temp_dict.keys()):
                temp_dict[i] = temp_dict[i].where(pd.notnull(temp_dict[i]), None)

            for i in range(len(temp_dict['代號'])):
                # print(f"index= '{i}'")
                cursor.execute(sql, (
                    None, str(temp_dict['代號'].iloc[i]), temp_dict['成交'].iloc[i],temp_dict['漲跌價'].iloc[i], temp_dict['漲跌幅'].iloc[i], temp_dict['成交張數'].iloc[i], temp_dict['成交額(百萬)'].iloc[i], temp_dict['PER'].iloc[i],
                    temp_dict['5日均線'].iloc[i],temp_dict['10日均線'].iloc[i], temp_dict['20日均線'].iloc[i], temp_dict['60日均線'].iloc[i],
                    temp_dict['RSI6(日)'].iloc[i], temp_dict['RSI12(日)'].iloc[i],temp_dict['RSI6(週)'].iloc[i], temp_dict['RSI12(週)'].iloc[i],
                    temp_dict['K值(日)'].iloc[i], temp_dict['D值(日)'].iloc[i],temp_dict['K值(週)'].iloc[i], temp_dict['D值(週)'].iloc[i],temp_dict['K值(月)'].iloc[i], temp_dict['D值(月)'].iloc[i],
                    temp_dict['5日均價乖離率'].iloc[i], temp_dict['10日均價乖離率'].iloc[i],temp_dict['20日均價乖離率'].iloc[i], temp_dict['60日均價乖離率'].iloc[i],
                    temp_dict['OSC(日)'].iloc[i], temp_dict['OSC(週)'].iloc[i], temp_dict['OSC(月)'].iloc[i],temp_dict['OSC(季)'].iloc[i],stock_type))
            conn.commit()
        elif deal  == "基本面":
            print(f" try to insert stock_type '{deal}'data into DB...")

            temp_dict = stock_info_dict_df[stock_type]['basic_info'][deal]

            for i in list(temp_dict.keys()):
                temp_dict[i] = temp_dict[i].where(pd.notnull(temp_dict[i]), None)

            for i in range(len(temp_dict['代號'])):
                # print(f"index= '{i}'")
                cursor.execute(sql, (
                    None, str(temp_dict['代號'].iloc[i]), temp_dict['成交'].iloc[i],temp_dict['漲跌價'].iloc[i], temp_dict['漲跌幅'].iloc[i],
                    temp_dict['開盤'].iloc[i], temp_dict['最高'].iloc[i], temp_dict['最低'].iloc[i],temp_dict['振幅(%)'].iloc[i],
                    temp_dict['成交張數'].iloc[i], temp_dict['成交額(百萬)'].iloc[i], temp_dict['PER'].iloc[i],
                    temp_dict['融資增減'].iloc[i],temp_dict['借券賣出增減'].iloc[i], temp_dict['借券賣出餘額'].iloc[i],
                    temp_dict['營收成長(%)'].iloc[i], temp_dict['毛利成長(%)'].iloc[i],temp_dict['24M06年增率'].iloc[i],
                    temp_dict['季獲利能力_獲利能力 (年增減統計)_EPS(元)'].iloc[i], temp_dict['近四季獲利能力_獲利能力 (年增減統計)_EPS(元)'].iloc[i],temp_dict['年獲利能力_獲利能力_EPS(元)'].iloc[i],
                    temp_dict['現金股利'].iloc[i], temp_dict['股票股利'].iloc[i]))







            conn.commit()

    except Exception as e:
        print('Get DB Connection Exception Occurs code no.=', e)
        if DB_conn.is_connection_alive(conn):
            print("Exception Occurs,Close the db connection")
            cursor.close()
            conn.close()
        return None

    # finally:
    #     if DB_conn.is_connection_alive(conn):
    #         print("Finally Occurs,Close the db connection")
    #         cursor.close()
    #         conn.close()
def initial_data(file_path):

    excel_df = pd.read_excel(file_path)
    new_excel_df=pd.DataFrame(columns=['日期','開盤','最高','最低','收盤','漲跌','漲跌(%)','振幅(%)','成交(億元)','成交均張','外資買賣超(億元)','投信買賣超(億元)','自營買賣超(億元)','合計買賣超(億元)','融資 (億元)餘額','融資 (億元)增減','融券 (萬張)餘額','融券 (萬張)增減'])
    print(list(excel_df.index))
    print(list(excel_df.columns))
    print('length_of_excel_df_columns=',len(list(excel_df.columns)))
    print("length of new_excel columns=",len(list(new_excel_df.columns)))
    for i in range(1,len(excel_df.index)):
        temp_info = []

        for j in range(0,len(excel_df.columns)-1):

            if isinstance(excel_df.iloc[i][j], str) and j==0:
                print("i=",i)

                if "/" in excel_df.iloc[i][j] :
                    temp_date=str(excel_df.iloc[i][-1])+"/"+excel_df.iloc[i][j]

                    temp_date = datetime.datetime.strptime(temp_date, '%Y/%m/%d')
                    temp_info.append(temp_date)
                elif "月" in excel_df.iloc[i][j] :
                    month=excel_df.iloc[i][j][:excel_df.iloc[i][j].find("月")]
                    day=excel_df.iloc[i][j][excel_df.iloc[i][j].find("月")+1:excel_df.iloc[i][j].find("日")]
                    temp_date = str(excel_df.iloc[i][-1]) + "/" + month+"/"+day

                    temp_date = datetime.datetime.strptime(temp_date, '%Y/%m/%d')
                    temp_info.append(temp_date)

            elif j==0:
                # .strftime("%Y/%m/%d %H:%M:%S").strftime("%m")
                # print("excel_df.iloc[i][-1]=",excel_df.iloc[i][-1])
                year=str(excel_df.iloc[i][-1])
                # print('year=',year)
                month=str(excel_df.iloc[i][j].month)
                day=str(excel_df.iloc[i][j].day)
                temp_date=year+"/"+month+'/'+day
                # print("temp_date=",temp_date)
                temp_date = datetime.datetime.strptime(temp_date, '%Y/%m/%d')
                temp_info.append(temp_date)
                # datetime.datetime.strptime(another_dts, "%%d")
            else:
                temp_info.append(excel_df.iloc[i][j])
            # print('length of temp_info=', len(temp_info))
        # print(temp_info)

        new_excel_df = new_excel_df.append(pd.Series(temp_info,index=list(new_excel_df.columns)), ignore_index=True)
        # print(temp_info)

    print("new_excel_df=")
    print(new_excel_df)
    new_excel_df.to_excel(file_path,index=False)



# if __name__ == '__main__':
    # OSC_path = 'D:/PycharmProjects/stock/stock_crawl_data/K_Chart_OSC.xlsx'
    # OTC_path = 'D:/PycharmProjects/stock/stock_crawl_data/K_Chart_OTC.xlsx'
    # initial_data(OSC_path)
    # initial_data(OTC_path)
