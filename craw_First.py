import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import pandas as pd
import time
from dotenv import load_dotenv
from supabase import create_client, Client

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


def main():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    service = Service('D:\\chromedriver-win64\\chromedriver.exe')  

    driver = webdriver.Chrome(service=service, options=options)

    # ETF 代碼清單
    etf_list = ['0050','0051','0052','0053','0055','0056','0057','006203','006204','006208',
    '00690','00692','00701','00713','00730','00728',
    '00731','00733','00850','00878','00881',
    '00891','00892','00894','00896','00901',
    '00900','00904','00905','00907','00912','00913','00915','00919','00918','00921','00923','00922',
    '00927','00929','00930','00932','00934','00935','00936','00939','00940','00944','00946','00943',
    '00947','00952','00961','00938','00962','009803','009802','009804','00980A','009808','00982A','00981A','00984A','00985A']

    #etf_list = ['00980A','009808','00982A','00981A','00984A','00985A']

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

        try:
            response = supabase.table("etf_holdings").insert(records).execute()
            if isinstance(response.data, dict) and "_raw_error" in response.data:
                print(f"{etf_code} 寫入失敗，錯誤訊息：{response.data['message']}")
            else:
                print(f"{etf_code} 成功寫入 {len(records)} 筆資料")
        except Exception as e:
            print(f"{etf_code} 寫入時發生錯誤:", e)

    driver.quit()

if __name__ == "__main__":
    main()
