# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 22:22:12 2024

@Author: HBBOY
@School: Sanda University
@Email: gao8888@88.com
                                                                   

"""
import tushare as ts  
import pandas as pd  
import time  
from tqdm import tqdm  
  
# 设置Tushare的token  
ts.set_token('Your Token')  
  
# 初始化pro接口  
pro = ts.pro_api()  
  
# 定义起始和结束日期  
start_date = '20230101'  
end_date = '20240227'  
filename=f'{start_date}-{end_date}'
  
# 获取指定日期范围内的交易日历  
trade_cal = pro.trade_cal(exchange='', is_open='1', start_date=start_date, end_date=end_date, fields='cal_date')  
trade_dates = trade_cal  
trade_dates.to_excel(f'{filename}_trade_dates.xlsx', index=False) 
print(f"交易日获取成功，已写入{filename}_trade_dates.xlsx")
# 初始化一个空的DataFrame，用于存储所有数据  
all_data = pd.DataFrame()  

#tqdm实现进度显示  
# trade_dates是一个DataFrame，要遍历它的所有行
for index in tqdm(trade_dates.index, desc='正在获取···'):
    # 获取指定日期的股票数据
    df = pro.daily(trade_date=trade_dates.iloc[index]['cal_date'])  # 每行有一个'cal_date'列
    
    # 将获取的数据追加到all_data DataFrame中(append方法已废弃，使用concat)
    all_data = pd.concat([all_data, df], ignore_index=True)
    
    # 在每次迭代后暂停一段时间，以避免过于频繁的请求（根据实际需要调整）
    time.sleep(1) 

#数据量太大，Excel工作表的最大行数是1048576行，不直接写入
# # 将结果保存到Excel文件中  
# all_data.to_excel(f'{filename}_result.xlsx', index=False)  
  
# print(f"数据获取成功，已写入{filename}_result.xlsx")

chunk_size = 200000
total_chunks = (len(all_data) // chunk_size) + bool(len(all_data) % chunk_size)
current_chunk = 0

for i in tqdm(range(0, len(all_data), chunk_size), total=total_chunks, desc='正在写入···'):
    chunk_df = all_data.iloc[i:i+chunk_size]

    # 计算并生成当前文件名
    filename = f'{start_date}-{end_date}_result_{current_chunk}.xlsx'

    # 写入Excel文件
    chunk_df.to_excel(filename, index=False)

    # 更新计数器
    current_chunk += 1

print("数据已按每20万行分割并保存到多个文件中")