from datetime import datetime
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import pandas as pd
import time
import requests, json
from dotenv import load_dotenv
from supabase import create_client, Client
from types import MappingProxyType


load_dotenv()

# Supabase 設定（改成你的 URL & KEY）
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)


def parse_ratio(r):
    if not r or r == '%' or r.strip() == '':
        return 0.0
    try:
        return float(r.strip('%'))
    except:
        return 0.0


def fetch_price_map():
    url = 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_AVG_ALL'
    response = requests.get(url)
    data = response.json()

    # 建立 {stock_code: avg_price} 的字典
    price_map = {}
    for item in data:
        try:
            price_map[item["Code"]] = float(item["MonthlyAveragePrice"])
        except (KeyError, ValueError):
            continue
    return MappingProxyType(price_map)


# 🌍 全域常數，不可變動
try:
    PRICE_MAP = fetch_price_map()
except Exception as e:
    print("取得 PRICE_MAP 失敗：", e)
    PRICE_MAP = MappingProxyType({})


def price_lookup_func(stock_code):
    avg_price = PRICE_MAP.get(stock_code)

    if avg_price is None:
        print(f"查無股票代號 {stock_code}")
        return 1e-3
    
    # 根據股價動態調整閾值
    if avg_price >= 1000:
        threshold =  1e-4    # 0.0001 = 0.01%
    elif avg_price >= 500:
        threshold = 2e-4    # 0.0002 = 0.02%
    elif avg_price >= 100:
        threshold = 5e-4    # 0.0005 = 0.05%
    elif avg_price >= 50:
        threshold = 1e-3    # 0.001  = 0.1%
    else:
        threshold = 2e-3
     
    return threshold

def find_deltas(old_list, new_list):
    old_map = {r['stock_code']: r for r in old_list}
    new_map = {r['stock_code']: r for r in new_list}

    delta_records = []
    

    # 新增或變動
    for stock_code, new_rec in new_map.items():
        old_rec = old_map.get(stock_code)
        if not old_rec:
            delta = new_rec.copy()
            delta['new_holding_units'] = delta.pop('holding_units', None)
            delta['new_holding_ratio'] = delta.pop('holding_ratio', None)
            
            delta.pop('unit', None)  # 多餘的，保險起見 ， 確保移除不需要的欄位

            delta['change_type'] = 'added'
            delta_records.append(delta)
        else:
            # 比較持有數或權重是否有改變（NaN 比較需特別處理）
            old_units = old_rec.get('holding_units')
            new_units = new_rec.get('holding_units')
            old_ratio = old_rec.get('holding_ratio')
            new_ratio = new_rec.get('holding_ratio')

            units_diff = old_units != new_units
            ratio_diff = False

            # 動態調整閾值
            threshold = price_lookup_func(stock_code)

            if old_ratio is None and new_ratio is None:
                ratio_diff = False
            elif (old_ratio is None) != (new_ratio is None):
                ratio_diff = True
            else:
                ratio_diff = abs(old_ratio - new_ratio) > threshold  #動態調整閾值
             

            if units_diff or ratio_diff:
                delta = new_rec.copy()
                # 改欄位名稱
                delta['new_holding_units'] = delta.pop('holding_units', None)
                delta['new_holding_ratio'] = delta.pop('holding_ratio', None)

                delta.pop('unit', None)  # 多餘的，保險起見 ， 確保移除不需要的欄位

                delta['change_type'] = 'changed'
                delta['old_holding_units'] = old_units
                delta['old_holding_ratio'] = old_ratio
                delta_records.append(delta)

    # 刪除
    for stock_code, old_rec in old_map.items():
        if stock_code not in new_map:
            delta = old_rec.copy()
            delta['new_holding_units'] = delta.pop('holding_units', None)
            delta['new_holding_ratio'] = delta.pop('holding_ratio', None)
            delta.pop('unit', None)  # 多餘的，保險起見 ， 確保移除不需要的欄位

            delta['change_type'] = 'removed'
            delta_records.append(delta)

    return delta_records


def main():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    service = Service('D:\\chromedriver-win64\\chromedriver.exe')  

    driver = webdriver.Chrome(service=service, options=options)

    # ETF 代碼清單
    etf_list = ['0051','0052','0053','0055']

    for etf_code in etf_list:
        url = f'https://www.pocket.tw/etf/tw/{etf_code}/fundholding?page&parent&source='
        driver.get(url)
        time.sleep(5)

        table = driver.find_element(By.CSS_SELECTOR, "table.cm-table__table")
        headers = [th.text.strip() for th in table.find_elements(By.CSS_SELECTOR, "thead th")]
        rows = []
        for tr in table.find_elements(By.CSS_SELECTOR, "tbody tr"):
            cells = [td.text.strip() for td in tr.find_elements(By.TAG_NAME, "td")]
            rows.append(cells)

        df = pd.DataFrame(rows, columns=headers)
        print(f"{etf_code} 資料筆數：", len(df))

        df['權重'] = df['權重'].apply(parse_ratio)
        df['持有數'] = df['持有數'].str.replace(',', '').astype('Int64')
        df['單位'] = df['單位'].fillna('')

        # 加這行，把 NaN 轉 None，避免 JSON 傳輸錯誤
        df = df.where(pd.notnull(df), None)



        records = [{
            'etf_code': etf_code,
            'stock_code': row['代號'],
            'stock_name': row['名稱'],
            'holding_ratio': row['權重'],
            'holding_units': row['持有數'],
            'unit': row['單位'],
        } for _, row in df.iterrows()]



        # 取得舊資料
        old_resp = supabase.table("etf_holdings").select("*").eq("etf_code", etf_code).execute()
        old_records = old_resp.data if old_resp.data else []

        # 找異動
        delta_records = find_deltas(old_records, records)


        
        if delta_records:
            print(f"{etf_code} 發現 {len(delta_records)} 筆異動，寫入差異表")
            #supabase.table("etf_holdings_delta").insert(delta_records).execute()
        else:
            print(f"{etf_code} 無異動")

        try:                  
            # upsert 最新資料
            print("更新主檔資料成功") 
            #supabase.table("etf_holdings").upsert(records, on_conflict="etf_code,stock_code,stock_name").execute()  
        except Exception as e:
            print(f"Error upserting records: {e}") 
        

    driver.quit()

if __name__ == "__main__":
    main()
