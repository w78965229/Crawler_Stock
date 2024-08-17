import  os
import pandas as pd
import os, fnmatch
import math
def get_excel_list():
    print("get_excel_list")
    path = "D:\PycharmProjects\stock\stock_crawl_data"
    transform_excel_folder="DataDealing"
    # file_list = os.listdir(path)
    file_list=fnmatch.filter(os.listdir('.'), 'stock_info_*.xlsx')
    print(file_list)
    print('length of file_list = {}'.format(len(file_list)))

    cnt=0
    file_dict={}
    stock_number_list=[]
    stock_index_list = []
    for file in file_list:


        if cnt <5 :
            # 如果文件扩展名为 .xlsx，则读取该文件
            # if file.endswith('.xlsx') and file.startswith('stock_info_'):
            #     df = pd.read_excel(file)
            #     df = pd.read_excel(file,parse_dates=['日期'],
            #                        date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d'))
                print(str(file) + ' is started')

                df = pd.read_excel(file, parse_dates=['日期'])

                df=df.rename(columns={"日期": "DATE", "代號": "STOCK",'公司':'COMPANY','產業類別':'GROUP','股價':'VALUE','成交量':'QTY','投信買賣超':'ST',\
                                   '外資買賣超':'FI','自營買賣超':'SET','本益比':'PE_Ratio'

                                   })
                df['VALUE_LOG']=0.0
                df['VALUE'] = df['VALUE'].astype(float)
                df['VALUE_LOG'] = df['VALUE_LOG'].astype(float)
                df['ID'] = df['STOCK']
                for i in df.index.tolist():
                    if df.at[i,'VALUE']<=0:
                        # print("i=,",i," df.at[i,'VALUE']=", df.at[i, 'VALUE'], ' type=', type(df.at[i, 'VALUE']))
                        df.at[i, 'VALUE_LOG'] =0.0
                    else:
                        df.at[i,'VALUE_LOG'] = math.log(df.at[i,'VALUE'])

                file_dict[file]= df.set_index('STOCK').T
                '''日期	代號	公司 產業類別	股價		成交量	投信買賣超	外資買賣超	自營買賣超	本益比	'''
                file_dict[file] = file_dict[file].loc[['DATE','COMPANY','GROUP','VALUE','QTY','ST','FI','SET','PE_Ratio','VALUE_LOG','ID'],:]
                print(str(file)+' is finished')
                cnt+=1
        if cnt == 1:
            stock_number_list = file_dict[file].columns.tolist()
            stock_index_list = file_dict[file].index.tolist()
    print("stock_number_list=",stock_number_list)
    print("stock_index_list=",stock_index_list)
    dict_stock={}
    for stock in stock_number_list:
        dict_stock[stock]=pd.DataFrame(index= stock_index_list)
        stock_date_price = {}
        for dict_key,dict_val in file_dict.items():
            if stock in dict_val.columns.tolist():#確認該股票有存在在那天的股票資訊裡面。
                dict_stock[stock] = pd.concat([dict_stock[stock],dict_val[str(stock)]],axis=1)

        # dict_stock[stock]=pd.concat([dict_stock[stock],stock_date_price])

        # print("dict_stock[stock]=",dict_stock[stock])
        # print('COMPANY=',dict_stock[stock].loc['COMPANY',0])

        dict_stock[stock]=dict_stock[stock].T.reset_index(drop=True)
        '''to_excel_check'''
        # excel_name=stock
        # if str(dict_stock[stock].at[0,'COMPANY'])!='nan':
        #     excel_name = stock + '_'+str(dict_stock[stock].at[0,'COMPANY'])
        #
        # dict_stock[stock].to_excel(os.path.join(path,os.path.join(transform_excel_folder,excel_name+".xlsx")))
    print("columns=",dict_stock['1476'].columns.tolist())
    print("index=", dict_stock['1476'].index.tolist())
    print(dict_stock['1476'])

    return dict_stock


def get_OSC_OTC_info():
    print("get_excel_list")
    path = "D:\PycharmProjects\stock\stock_crawl_data"
    # file_list = os.listdir(path)
    K_Chart_OSC_file_name='K_Chart_OSC.xlsx'
    K_Chart_OTC_file_name='K_Chart_OTC.xlsx'

    OSC_file = pd.read_excel(os.path.join(path,K_Chart_OSC_file_name), parse_dates=['日期'])
    OTC_file = pd.read_excel(os.path.join(path,K_Chart_OTC_file_name), parse_dates=['日期'])

    print("OSC_file=",OSC_file.columns.tolist())


    OSC_df = OSC_file.rename(columns={"日期": "DATE", "最高": "HIGH", '最低': 'LOW', '收盤': 'PRICE', '漲跌': 'DELTA',
                            '漲跌(%)': 'DELTA_P',
                            '成交均張': 'CASH_UNIT',
                            '成交(億元)': 'CASH',
                            '投信買賣超(億元)' :'ST',
                            '外資買賣超(億元)': 'FI', '自營買賣超(億元)': 'SET'})

    OTC_df = OTC_file.rename(columns={"日期": "DATE", "最高": "HIGH", '最低': 'LOW', '收盤': 'PRICE', '漲跌': 'DELTA',
                                    '漲跌(%)': 'DELTA_P',
                                    '成交均張': 'CASH_UNIT',
                                    '成交(億元)': 'CASH',
                                    '投信買賣超(億元)': 'ST',
                                    '外資買賣超(億元)': 'FI', '自營買賣超(億元)': 'SET'})

    OSC_df['PRICE_LOG'] = 0.0
    OSC_df['PRICE_LOG'] = OSC_df['PRICE_LOG'].astype(float)
    OSC_df['PRICE'] = OSC_df['PRICE'].astype(float)

    for i in OSC_df.index.tolist():
        if OSC_df.at[i, 'PRICE'] <= 0:
            # print("i=,",i," df.at[i,'VALUE']=", df.at[i, 'VALUE'], ' type=', type(df.at[i, 'VALUE']))
            OSC_df.at[i, 'PRICE_LOG'] = 0.0
        else:
            OSC_df.at[i, 'PRICE_LOG'] = math.log(OSC_df.at[i, 'PRICE'])


    OTC_df['PRICE_LOG'] = 0.0
    OTC_df['PRICE_LOG'] = OTC_df['PRICE_LOG'].astype(float)
    OTC_df['PRICE'] = OTC_df['PRICE'].astype(float)

    for i in OTC_df.index.tolist():
        if OTC_df.at[i, 'PRICE'] <= 0:
            # print("i=,",i," df.at[i,'VALUE']=", df.at[i, 'VALUE'], ' type=', type(df.at[i, 'VALUE']))
            OTC_df.at[i, 'PRICE_LOG'] = 0.0
        else:
            OTC_df.at[i, 'PRICE_LOG'] = math.log(OTC_df.at[i, 'PRICE'])

    OSC_df= OSC_df.loc[ :,['DATE', 'HIGH', 'LOW', 'PRICE', 'DELTA', 'DELTA_P', 'CASH_UNIT', 'CASH', 'ST', 'FI', 'SET','PRICE_LOG'],]
    OTC_df= OTC_df.loc[ :,['DATE', 'HIGH', 'LOW', 'PRICE', 'DELTA', 'DELTA_P', 'CASH_UNIT', 'CASH', 'ST', 'FI', 'SET','PRICE_LOG'],]





    print("OSC_df=",OSC_df)
    return OSC_df,OTC_df
def data_dealing(STOCK_DICT,OSC_DF,OTC_DF):
    print("data_dealing with input data stock,osc,otc")
    # print("this is stock_dict",STOCK_DICT)
    '''init'''
    osc_df= OSC_DF
    print("STOCK_DICT.keys().tolist()=",STOCK_DICT.keys())
    #STOCK_DICT.format='2022-07-22'
    # print(STOCK_DICT['1476']['DATE'=='2022-07-22'].index[0])
    print('osc_df=\n',osc_df)
    # print("STOCK_DICT['1476']=",STOCK_DICT['1476'])
    # print(STOCK_DICT['1476'].at[(STOCK_DICT['1476']['DATE']=='2022-07-22').index.tolist()[0],'VALUE_LOG'])
    stock_corr_list=['DATE','PRICE_LOG','DELTA','CASH', 'ST', 'FI', 'SET']
    to_excel_stock_list=['2330','2317','6182']
    print(" STOCK_DICT[0050].loc[:10+1,'VALUE_LOG'].std()=", STOCK_DICT['0050'].loc[:10 + 1, 'VALUE_LOG'].std())

    for i in STOCK_DICT.keys():
        STOCK_DICT[i]['STDEV']=  0.0
        STOCK_DICT[i]['STDEV'] = STOCK_DICT[i]['STDEV'].astype(float)

        # osc_df[i+'_cor'] = 0.0
        osc_df[i + '_price'] = 0.0
        temp_df=STOCK_DICT[i].loc[:,['DATE','VALUE','VALUE_LOG']]
        temp_df = temp_df.rename(columns={"VALUE": str(i)+"_VALUE", "VALUE_LOG": str(i)+"_VALUE_LOG"})
        # print(temp_df)
        osc_df=osc_df.merge(temp_df, how='left')
        stock_corr_list.append(str(i)+"_VALUE_LOG")

        for j in STOCK_DICT[i].index.tolist():
            STOCK_DICT[i].at[j,'STDEV'] =  STOCK_DICT[i].loc[:j+1,'VALUE_LOG'].std()
        if i in to_excel_stock_list:
            STOCK_DICT_corr_df = STOCK_DICT[i].loc[:,['DATE','VALUE','QTY','ST','FI','SET','PE_Ratio','VALUE_LOG']].corr()
            STOCK_DICT_corr_df.to_excel(os.path.join("D:\PycharmProjects\stock\stock_crawl_data\DataDealing",str(i)+"_"+STOCK_DICT[i].at[0,'COMPANY']+"_corr.xlsx"))

    '''沒有值的value_log部分補0'''
    osc_df=osc_df.fillna(0)
    osc_df=osc_df[(osc_df['DATE']>=STOCK_DICT['0050'].at[0,'DATE']) and (osc_df['DATE']<STOCK_DICT['0050'].at[-1,'DATE']) ].reset_index(drop=True)

    print("stock_corr_list=",stock_corr_list)

    stock_corr_df= osc_df.loc[:,stock_corr_list]
    stock_corr_df=stock_corr_df.corr()

    # print(osc_df.columns.tolist())

    # print(stock_corr_df)
    # stock_corr_df.to_excel("D:\PycharmProjects\stock\stock_crawl_data\DataDealing\stock_corr_df.xlsx")
    # osc_df.to_excel("D:\PycharmProjects\stock\stock_crawl_data\DataDealing\emp.xlsx")

# dict_stock[stock].to_excel(os.path.join(path,os.path.join(transform_excel_folder,excel_name+".xlsx")))

