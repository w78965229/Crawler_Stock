import datetime
import os
import pandas as pd
import time
# -*- coding: utf-8 -*-
#coding=gbk
def is_file_exit(file_path):
    print("is_file_exit")
    if os.path.exists(file_path):
        return True
    else:
        return False
'''
func : is_file_first_today
檢查是否該新增此筆資料進入excel成為今天的卡控
假設為當日執行之程式，若抓取之資料非當日之資訊，則不放入EXCEL內
return True 不寫入excel
else 則需寫入excel
input: file_path =>當前存取的歷史大盤資料 預設是資料最新會是前一天的資料。 time_stamp
       today: 當下程式執行的日期 預設為當日。 string 
       
       
'''
def is_file_first_today(file_path,data_datecode,category):
    file=pd.read_excel(file_path,engine='openpyxl')
    # print("file.iloc[0,0]=",file.iloc[0,0],' type=',type(file.iloc[0,0]))
    # print("today=",today,' type=',type(today))
    if isinstance(file.iloc[0,0], str): #如果是string格式

        first_header = file.iloc[0, 0][:11].strip() #則取前面日期即可，不要時間。 那就看是不是datetime.date.today().strftime('%Y/%m/%d')

    else:
        first_header = datetime.datetime.strftime( file.iloc[0,0], "%Y/%m/%d")# 轉成字串 2022/08/21
    print("first_header=",first_header) #first_header= excel最新的資料。 如果與今天的日期相同，那就insert資料到excel了
    # print("today=",today)

    #如果今天的時間比EXCEL最晚的資料來的來的大 就要放進檔案裡

    if first_header==data_datecode:
        print("category="+category+"  :the first header of "+file_path+" is "+first_header + "等於爬蟲資料日期"+data_datecode+" 故不須加入檔案之中")
        return True
    else:
        print("category="+category+"  :the first header of "+file_path+" is "+first_header + "不等於爬蟲資料日期"+data_datecode+" 需要加入檔案之中")
        return False

def is_today_week_day_execute():
    week_day=datetime.datetime.today().weekday()
    if week_day<=5:#5==禮拜六,6==禮拜天,0=禮拜一
        return week_day,True
    else:
        return week_day,False


