# usr/bin/env/python38
# coding: utf-8
# Author: Maxincer
# Create DateTime: 20200916T215500

"""
将从市场部给出的微文档“新股卖出工作表”
https://doc.weixin.qq.com/txdoc/excel?scode=APkAwQdlAAcq9NYNxoAFQAgAb5ACc&docid=e2_AGoAgAb5ACcjqjSRC8sTeyXK0QcXi&type=1
转化为由IT部门提供的dma.exe程序读取文件 data.csv

Assumption:
    1. 合规假设
        1. 网下新股申购策略，产品与新股交易账户为一对一关系


abbr:
    POP: Public Offering Price
    IOD: Initial Offering Date
"""
from datetime import datetime

import orjson
import pandas as pd
from pymongo import MongoClient
import redis


class get_data_csv_for_dma:
    def __init__(self):
        dt_today = datetime.today()
        self.str_today = dt_today.strftime('%Y%m%d')
        # self.str_today = '20201214'

        self.server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        self.server_redis_187_110 = redis.Redis(host='47.103.187.110', port=6379, password='Ms123456')

        self.db_global = self.server_mongodb['global']
        self.col_trdcalendar = self.db_global['trade_calendar']
        self.list_str_trdcalendar = []
        for _ in self.col_trdcalendar.find():
            self.list_str_trdcalendar += _['Data']
        idx_str_today = self.list_str_trdcalendar.index(self.str_today)
        self.str_last_trddate = self.list_str_trdcalendar[idx_str_today - 1]
        self.str_next_trddate = self.list_str_trdcalendar[idx_str_today + 1]
        self.str_last_last_trddate = self.list_str_trdcalendar[idx_str_today - 2]
        self.str_next_next_trddate = self.list_str_trdcalendar[idx_str_today + 2]

        self.fpath_ossipo_ws = 'D:/data/ossipo/ossipo.xlsx'
        self.fpath_output_data_for_dma = 'D:/projects/trd_ossipo/dma/data.csv'
        db_basicinfo = self.server_mongodb['basicinfo']
        self.col_acctinfo = db_basicinfo['acctinfo']

    def get_data_csv_for_dma(self, str_trddate):
        df_ossipo_trdplan = pd.read_excel(
            self.fpath_ossipo_ws,
            sheet_name='ossipo_trdplan',
            dtype={
                '产品编号': str,
                '发行价': float,
                '中签数量': int,
                '锁定数量': int,
                '订单批号': str,
                'OrdType': str
            },
            converters={'新股代码': lambda x: str(x).zfill(6)}
        )
        list_dicts_ossipo_trdplan = df_ossipo_trdplan.to_dict('records')
        list_dicts_data_for_dma = []
        for dict_ossipo_trdplan in list_dicts_ossipo_trdplan:
            str_trddate_in_plan = dict_ossipo_trdplan['订单批号']
            if str_trddate_in_plan == str_trddate:
                user = 'test03'
                prdcode = dict_ossipo_trdplan['产品编号']
                code = dict_ossipo_trdplan['新股代码']
                pop = dict_ossipo_trdplan['发行价']
                ordtype = dict_ossipo_trdplan['OrdType']  # 根据需求抽象，指定业务规则如下: 如无标记，则按照发行价报单；如有标记，则按照标记的类型报单
                if code[0] in ['6']:
                    secid_by_huat_datadict = f'market_{code}.SH'
                elif code[0] in ['3', '0']:
                    secid_by_huat_datadict = f'market_{code}.SZ'
                else:
                    raise ValueError('Unknown stock code in ossipo.')

                if ordtype in ['2%']:
                    dict_mdentry = orjson.loads(self.server_redis_187_110.get(secid_by_huat_datadict).decode('utf-8'))
                    lastpx = dict_mdentry['LastPx'] / 10000
                    minpx = dict_mdentry['MinPx'] / 10000
                    price = max(round(lastpx * 0.98, 2), minpx)
                elif ordtype in ['minpx']:
                    dict_mdentry = orjson.loads(self.server_redis_187_110.get(secid_by_huat_datadict).decode('utf-8'))
                    minpx = dict_mdentry['MinPx'] / 10000
                    price = minpx
                else:
                    price = pop
                quota = 0 - (dict_ossipo_trdplan['中签数量'] - dict_ossipo_trdplan['锁定数量'])
                short = 0
                # 合规假设1: 网下新股申购策略，产品与新股交易账户为一对一关系
                for dict_basicinfo in self.col_acctinfo.find({'DataDate': self.str_today, 'PrdCode': prdcode}):
                    if dict_basicinfo['StrategiesAllocationByAcct']:
                        list_strategies = dict_basicinfo['StrategiesAllocationByAcct'].split(';')
                    else:
                        list_strategies = []
                    if 'OSSIPO_newshares' in list_strategies:
                        acctidbyxxq = dict_basicinfo['AcctIDByXuXiaoQiang4Trd']
                        dict_data_for_dma = {
                            'user': user,
                            'code': code,
                            'accountname': acctidbyxxq,
                            'quota': quota,
                            'price': price,
                            'short': short
                        }
                        list_dicts_data_for_dma.append(dict_data_for_dma)
                    else:
                        continue
        df_data_for_dma = pd.DataFrame(list_dicts_data_for_dma)
        df_data_for_dma.to_csv(self.fpath_output_data_for_dma, index=False)
        i_orders = len(list_dicts_data_for_dma)
        print(f'data.csv finished: {i_orders} orders planned on {str_trddate} generated.')

    def run(self):
        self.get_data_csv_for_dma('202103080915')


if __name__ == '__main__':
    task = get_data_csv_for_dma()
    task.run()









