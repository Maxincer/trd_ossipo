#!usr/bin/env python3
# coding:utf-8
# Author: Maxincer
# Update Time: 20200422T1400 +0800

"""
Format trading csv from brokers including MorganStanley, CICC, CS.
"""
from datetime import date, datetime
import pandas as pd


class FmtTradeFilesFromDifferentBrokers:
    def __init__(self, str_date, list_broker_alias):
        self.str_date = str_date
        self.COLS = [
            'OrdStatus',
            'ExecTransType',
            'ClientOrderID',
            'Fill ID',
            'ID of Order Or Fill for Action',
            'LotNumber',
            'Symbol',
            'SecurityType',
            'Security Currency',
            'Security Description',
            'BuySellShortCover',
            'OpenClose',
            'IDSource',
            'SecurityID',
            'ISIN',
            'CUSIP',
            'SEDOL',
            'Bloomberg',
            'CINS',
            'WhenIssued',
            'IssueDate',
            'Maturity Date',
            'Coupon %',
            'ExecutionInterestDays',
            'AccruedInterest',
            'FaceValue',
            'RollableType',
            'Repo Currency',
            'Day Count Fraction / Repo Calendar',
            'RepoLoanAmount',
            'Trader',
            'OrderQty',
            'FillQty',
            'CumQty',
            'HairCut',
            'Avg Price',
            'FillPrice',
            'TradeDate',
            'TradeTime',
            'OrigDate',
            'Unused',
            'SettlementDate',
            'Executing User',
            'Comment',
            'Account',
            'Fund',
            'SubFund',
            'AllocationCode',
            'StrategyCode',
            'Execution Broker',
            'Clearing Agent',
            'ContractSize',
            'Commission',
            'FX Rate',
            'FWD FX points',
            'Fee',
            'CurrencyTraded',
            'SettleCurrency',
            'FX/BASE rate',
            'BASE/FX rate',
            'StrikePrice',
            'PutOrCall',
            'Derivative Expiry',
            'SubStrategy',
            'OrderGroup',
            'RepoPenalty',
            'CommissionTurn',
            'AllocRule',
            'PaymentFreq',
            'RateSource',
            'Spread',
            'CurrentFace',
            'CurrentPrincipalFactor',
            'AccrualFactor',
            'Tax Rate',
            'Expenses',
            'Fees',
            'PostCommAndFeesOnInit',
            'Implied Commission Flag',
            'Transaction Type',
            'Master Confrim Type',
            'Matrix Term',
            'EMInternalSeqNo.',
            'ObjectivePrice',
            'MarketPrice',
            'Stop Price',
            'NetConsdieration',
            'Fixing Date',
            'Delivery Instructions',
            'Force Match ID',
            'Force Match Type',
            'Force Match Notes',
            'Commission Rate for Allocation',
            'Commission Amount for Fill',
            'Expense Amount for Fill',
            'Fee Amount for Fill',
            'Standard Strategy',
            'Strategy Link Name',
            'Strategy Group',
            'Fill FX Settle Amount',
            'Reserved',
            'Reserved.1',
            'Deal Attributes',
            'Finance Leg',
            'Performance Leg',
            'Attributes',
            'Deal Symbol',
            'Initial margin type ',
            'Initial Margin Amount',
            'Initial margin CCY ',
            'Confirm Status',
            'Counterparty',
            'Trader Notes',
            'Convert Price\nto Settle Ccy',
            'Bond Coupon Type',
            'Generic Fees Enabled',
            'Generic Fees Listing',
            'Order Level Attributes',
            'Settling/Sub',
            'Confirmation Time',
            'Confirmation Means',
            'Payment Date'
        ]
        fpath_stockcode = 'Stock Code.csv'
        df_stockcode = pd.read_csv(fpath_stockcode).loc[:, ['RIC', 'Short_Name']].set_index('RIC')
        self.dict_ric_shortname = df_stockcode.to_dict()['Short_Name']
        self.list_broker_alias = list_broker_alias
        self.list_fpaths = [f'trade_{x}_{self.str_date}.csv' for x in self.list_broker_alias]

    @staticmethod
    def ric2cfd_eqidxswap(ric):
        list_splitted_ric = ric.split('.')
        if 0 <= int(list_splitted_ric[0]) <= 1000000 and list_splitted_ric[1] in ['SS', 'SZ', 'ZK', 'SH']:
            return 'CFD'
        elif ric in ['.CSIN00905']:
            return 'EQUIDXSWAP'
        else:
            raise ValueError('Unexpected value input.')

    @staticmethod
    def ric2cny_cnh(ric_brokeralias):
        """
        :param ric_brokeralias: 形如ric_brokeralias的str, eg.: 000000.SS_MS
        :return:
        str
            cny or cnh
        """
        list_ric_brokeralias = ric_brokeralias.split('_')
        ric = list_ric_brokeralias[0]
        __broker_alias = list_ric_brokeralias[1]
        list_splitted_ric = ric.split('.')
        if 0 <= int(list_splitted_ric[0]) <= 1000000 and list_splitted_ric[1] in ['SS', 'SZ', 'ZK', 'SH']:
            if list_splitted_ric[1] in ['SS', 'SZ']:
                return 'CNY'
            elif list_splitted_ric[1] in ['SH', 'ZK']:
                return 'CNH'
            else:
                raise ValueError('Wrong ric value.')
        elif ric in ['.CSIN00905']:
            if __broker_alias in ['MS']:
                return 'CNH'
            elif __broker_alias in ['CS', 'CICC']:
                return 'CNY'
        else:
            raise ValueError('Wrong ric or broker.')

    def fill_ratesource(self, ric):
        ret = self.ric2cfd_eqidxswap(ric)
        if ret == 'EQUIDXSWAP':
            return 226
        else:
            return None

    @staticmethod
    def fill_maturity_date_in_cicc(str_date):
        """
        业务层约定：输入trade date, 在此基础上增加一年
        :param str_date:
        :return:
        """
        list_str_date = str_date.split('/')
        year = int(list_str_date[0]) + 1
        month = int(list_str_date[1])
        day = int(list_str_date[2])
        __str_date = date(year, month, day).strftime('%Y%m%d')
        return __str_date

    @staticmethod
    def fmt_date_with_slash_in_jpm(str_date_with_slash):
        list_date = [int(x) for x in str_date_with_slash.split('/')]  # 改斜杠
        dt_date = date(list_date[0], list_date[1], list_date[2])
        __str_date = date.strftime(dt_date, '%Y%m%d')
        return __str_date

    @staticmethod
    def fmt_date_with_slash(str_date_with_slash):
        list_date = [int(x) for x in str_date_with_slash.split('/')]
        dt_date = date(list_date[2], list_date[0], list_date[1])
        __str_date = date.strftime(dt_date, '%Y%m%d')
        return __str_date

    @staticmethod
    def fmt_date_with_slash_ymd(str_date_with_slash_ymd):
        list_date = [int(x) for x in str_date_with_slash_ymd.split('/')]
        dt_date = date(list_date[0], list_date[1], list_date[2])
        __str_date = date.strftime(dt_date, '%Y%m%d')
        return __str_date

    @staticmethod
    def fmt_date_in_broker_cs(str_date_in_broker_cs):
        dt_date = datetime.strptime(str_date_in_broker_cs, '%b-%d-%Y')
        __str_date = dt_date.strftime('%Y%m%d')
        return __str_date

    def fmt_cicc(self, fpath):
        df_csv = pd.read_csv(fpath,
                             skiprows=2,
                             converters={'Gross Price (in Settle CCY)': lambda x: round(float(x), 4)})
        df_csv['Stock Code'] = df_csv['Stock Code'].apply(lambda x: x[:-1])
        df_ret = pd.DataFrame(data=[], columns=self.COLS)
        df_ret["Security Description"] = df_csv['Stock Code'].apply(lambda x: self.dict_ric_shortname[x])
        dict_buysellshortcover = {"Open B": "B", "Unwind B": "BC", "Unwind S": "S", "Open S": "SS"}
        df_ret["BuySellShortCover"] = df_csv["Open/Unwind"].str.cat(df_csv["B/S"], sep=" ").map(dict_buysellshortcover)
        df_ret["SecurityID"] = df_csv["Stock Code"]
        df_ret["Maturity Date"] = df_csv['Trade Date'].apply(self.fill_maturity_date_in_cicc)
        df_ret["OrderQty"] = df_csv["Qty"]
        df_ret["Avg Price"] = df_csv["Gross Price (in Settle CCY)"]
        df_ret["FillPrice"] = df_csv["Gross Price (in Settle CCY)"]
        df_ret["TradeDate"] = df_csv["Trade Date"].apply(self.fmt_date_with_slash_ymd)
        df_ret["SettlementDate"] = df_csv["Settlement Date"].apply(self.fmt_date_with_slash_ymd)
        df_ret["Commission"] = abs(round(df_csv["Qty"] * df_csv["Gross Price (in Settle CCY)"]
                                         * (df_csv['Counterparty Fee (bps)']) / 1e4, 4))
        df_ret["FX Rate"] = df_csv["FX"]
        df_ret["CurrencyTraded"] = (df_csv['Stock Code'] + '_CICC').apply(self.ric2cny_cnh)
        df_ret["Spread"] = 65
        df_ret["SecurityType"] = df_csv['Stock Code'].apply(self.ric2cfd_eqidxswap)
        df_ret["Security Currency"] = (df_csv['Stock Code'] + '_CICC').apply(self.ric2cny_cnh)
        df_ret["Execution Broker"] = 'CICC'
        df_ret["Clearing Agent"] = "CICC_SW"
        date_fmt = fpath.split("_")[-1][:-4]
        df_ret["ClientOrderID"] = ["{}I{}".format(date_fmt, int(x + 1e6 + 1)) for x in df_csv.reset_index().index]
        df_ret["Fill ID"] = ["{}I{}".format(date_fmt, int(x + 1e6 + 1)) for x in df_csv.reset_index().index]

        df_ret["OrdStatus"] = "N"
        df_ret["ExecTransType"] = 2
        df_ret["IDSource"] = "RIC"
        df_ret["Trader"] = "DFLT"
        df_ret["Fund"] = "MCO_MST"
        df_ret["SettleCurrency"] = "USD"
        df_ret["PaymentFreq"] = 4
        df_ret["RateSource"] = df_csv['Stock Code'].apply(self.fill_ratesource)
        df_ret["PostCommAndFeesOnInit"] = "N"
        df_ret.loc[df_ret["Maturity Date"] == "NaT", "Maturity Date"] = None
        return df_ret

    def fmt_cs(self, fpath):
        df_csv = pd.read_csv(fpath,
                             dtype={'Swap Shell Maturtiy Date': str, 'Swap Maturity/Termination Date': str},
                             converters={'Initial Price / Unwind Price': lambda x: round(float(x), 4)})
        df_ret = pd.DataFrame(data=[], columns=self.COLS)
        df_ret["Security Description"] = df_csv['Security ID'].apply(lambda x: self.dict_ric_shortname[x])
        dict_buysellshortcover = {"OPEN BUY": "B", "CLOSE BUY": "BC", "CLOSE SELL": "S", "OPEN SELL": "SS"}
        df_ret["BuySellShortCover"] = \
            df_csv["Open / Close"].str.cat(df_csv["Synthetic Buy / Sell"], sep=" ").map(dict_buysellshortcover)
        df_ret["SecurityID"] = df_csv["Security ID"]
        df_ret["Maturity Date"] = df_csv["Swap Maturity/Termination Date"].apply(self.fmt_date_in_broker_cs)
        df_ret["OrderQty"] = df_csv["No. of Shares/Units (Quantity)"]
        df_ret["Avg Price"] = df_csv["Initial Price / Unwind Price"]
        df_ret["FillPrice"] = df_csv["Initial Price / Unwind Price"]
        df_ret["TradeDate"] = df_csv["Trade Date"].apply(self.fmt_date_in_broker_cs)
        df_ret["SettlementDate"] = df_csv["Effective Date"].apply(self.fmt_date_in_broker_cs)
        df_ret["Commission"] = abs(round(df_csv["No. of Shares/Units (Quantity)"]
                                         * df_csv["Initial Price / Unwind Price"]
                                         * (df_csv['Initial/Final Swap Fee']) / 1e4, 4))
        df_ret["FX Rate"] = df_csv["Swap Exchange Rate"]
        df_ret["CurrencyTraded"] = (df_csv['Security ID'] + '_CS').apply(self.ric2cny_cnh)
        df_ret["Spread"] = df_csv["Spread bps"]
        df_ret["SecurityType"] = df_csv['Security ID'].apply(self.ric2cfd_eqidxswap)
        df_ret["Security Currency"] = (df_csv['Security ID'] + '_CS').apply(self.ric2cny_cnh)
        df_ret["Execution Broker"] = 'CS'
        df_ret["Clearing Agent"] = "CS_SW"
        date_fmt = fpath.split("_")[-1][:-4]
        df_ret["ClientOrderID"] = ["{}S{}".format(date_fmt, int(x + 1e6 + 1)) for x in df_csv.reset_index().index]
        df_ret["Fill ID"] = ["{}S{}".format(date_fmt, int(x + 1e6 + 1)) for x in df_csv.reset_index().index]

        df_ret["OrdStatus"] = "N"
        df_ret["ExecTransType"] = 2
        df_ret["IDSource"] = "RIC"
        df_ret["Trader"] = "DFLT"
        df_ret["Fund"] = "MCO_MST"
        df_ret["SettleCurrency"] = "USD"
        df_ret["PaymentFreq"] = 4
        df_ret["RateSource"] = df_csv['Security ID'].apply(self.fill_ratesource)
        df_ret["PostCommAndFeesOnInit"] = "N"
        df_ret.loc[df_ret["Maturity Date"] == "NaT", "Maturity Date"] = None
        return df_ret

    def fmt_ms(self, fpath):
        df_csv = pd.read_csv(fpath,
                             dtype={'Swap Shell Maturtiy Date': str},
                             converters={'Price (Gross) Swap Ccy': lambda x: round(float(x), 4)})
        df_ret = pd.DataFrame(data=[], columns=self.COLS)
        df_ret["Security Description"] = df_csv['Ric'].apply(lambda x: self.dict_ric_shortname[x])
        dict_buysellshortcover = {"Open Buy": "B", "Unwind Buy": "BC", "Unwind Sell": "S", "Open Sell": "SS"}
        df_ret["BuySellShortCover"] = \
            df_csv["Open/Unwind"].str.cat(df_csv["Buy/Sell"], sep=" ").map(dict_buysellshortcover)
        df_ret["SecurityID"] = df_csv["Ric"]
        df_ret["Maturity Date"] = df_csv["Swap Shell Maturtiy Date"].apply(self.fmt_date_with_slash)
        df_ret["OrderQty"] = df_csv["Stock Quantity"]
        df_ret["Avg Price"] = df_csv["Price (Gross) Swap Ccy"]
        df_ret["FillPrice"] = df_csv["Price (Gross) Swap Ccy"]
        df_ret["TradeDate"] = df_csv["Trade Date"].apply(self.fmt_date_with_slash)
        df_ret["SettlementDate"] = df_csv["Settlement Date"].apply(self.fmt_date_with_slash)
        df_ret["Commission"] = round(df_csv["Stock Quantity"] * df_csv["Price (Gross) Swap Ccy"] * (
                df_csv["Commission Charge"] + df_csv["Market Equivalent Charges"]) / 1e4, 4)
        df_ret["FX Rate"] = df_csv["FX Rate"]
        df_ret["CurrencyTraded"] = (df_csv['Ric'] + '_MS').apply(self.ric2cny_cnh)
        df_ret["Spread"] = df_csv["Lead Spread"]

        df_ret["OrdStatus"] = "N"
        df_ret["ExecTransType"] = 2
        df_ret["SecurityType"] = df_csv['Ric'].apply(self.ric2cfd_eqidxswap)
        df_ret["Security Currency"] = (df_csv['Ric'] + '_MS').apply(self.ric2cny_cnh)
        df_ret["IDSource"] = "RIC"
        df_ret["Trader"] = "DFLT"
        df_ret["Fund"] = "MCO_MST"
        df_ret["SettleCurrency"] = "USD"
        df_ret["PaymentFreq"] = 4
        df_ret["RateSource"] = df_csv['Ric'].apply(self.fill_ratesource)
        df_ret["PostCommAndFeesOnInit"] = "N"

        df_ret["Execution Broker"] = 'MS'
        df_ret["Clearing Agent"] = "MS_SW"

        date_fmt = fpath.split("_")[-1][:-4]
        df_ret["ClientOrderID"] = ["{}M{}".format(date_fmt, int(x + 1e6 + 1)) for x in df_csv.reset_index().index]
        df_ret["Fill ID"] = df_ret["ClientOrderID"]

        df_ret.loc[df_ret["Maturity Date"] == "NaT", "Maturity Date"] = None
        return df_ret

    def bbg2ric(self, str_bbg_code):
        list_str_bbg_code_split = str_bbg_code.split()
        secid = list_str_bbg_code_split[0]
        suffix = list_str_bbg_code_split[1]
        dict_suffix_map = {'C1': '.SH', 'C2': '.ZK', 'CG': '.SS', 'CS': '.SZ'}
        str_ric_suffix = dict_suffix_map[suffix]
        str_ric = secid + str_ric_suffix
        return str_ric

    def fmt_jpm(self, fpath):
        df_csv = pd.read_csv(fpath, converters={'Initial price': lambda x: round(float(x), 4)})
        df_ret = pd.DataFrame(data=[], columns=self.COLS)
        df_ret["Security Description"] = df_csv['RIC Code'].apply(lambda x: self.dict_ric_shortname[x])
        dict_buysellshortcover = {"LB": "B", "SB": "BC", "LS": "S", "SS": "SS"}
        df_ret["BuySellShortCover"] = df_csv["Long Short Indicator"].str.cat(df_csv["Synthetic Buy / Sell"]).map(dict_buysellshortcover)
        df_ret["SecurityID"] = df_csv["RIC Code"]
        df_ret["Maturity Date"] = df_csv['Termination Date'].apply(self.fmt_date_with_slash_in_jpm)
        df_ret["OrderQty"] = df_csv["Initial Quantity"]
        df_ret["Avg Price"] = df_csv["Initial price"]
        df_ret["FillPrice"] = df_csv["Initial price"]
        df_ret["TradeDate"] = df_csv["Trade Date"].apply(self.fmt_date_with_slash_in_jpm)
        df_ret["SettlementDate"] = df_csv["Effective Date"].apply(self.fmt_date_with_slash_in_jpm)
        dict_suffix2connect_qffi = {'.ZK': 'connect', '.SH': 'connect', '.SZ': 'qfii', '.SS': 'qffi'}
        df_csv['connect_qfii'] = df_csv['RIC Code'].apply(lambda x: dict_suffix2connect_qffi[x[-3:]])
        df_csv['fee_mark'] = df_csv['connect_qfii'] + df_csv['Synthetic Buy / Sell']
        dict_fee_mark2fee = {'connectB': 2.587, 'connectS': 12.587, 'qfiiB': 3.887, 'qfiiS': 13.887}
        df_csv['fee'] = df_csv['fee_mark'].map(dict_fee_mark2fee)
        df_ret["Commission"] = abs(round(df_csv["Initial Quantity"] * df_csv["Initial price"] * (df_csv['fee']) / 1e4, 4)).round(decimals=4)
        df_ret["FX Rate"] = 1 / df_csv["Initial FX Rate"]
        df_ret["CurrencyTraded"] = (df_csv['RIC Code'] + '_JPM').apply(self.ric2cny_cnh)
        df_ret["Spread"] = (df_csv['Additional Spread'] + df_csv['Long Financing Spread (%)']) * 100
        df_ret["SecurityType"] = df_csv['RIC Code'].apply(self.ric2cfd_eqidxswap)
        df_ret["Security Currency"] = (df_csv['RIC Code'] + '_JPM').apply(self.ric2cny_cnh)
        df_ret["Execution Broker"] = 'JPM'
        df_ret["Clearing Agent"] = "JPM_SW"
        date_fmt = fpath.split("_")[-1][:-4]
        df_ret["ClientOrderID"] = ["{}J{}".format(date_fmt, int(x + 1e6 + 1)) for x in df_csv.reset_index().index]
        df_ret["Fill ID"] = ["{}J{}".format(date_fmt, int(x + 1e6 + 1)) for x in df_csv.reset_index().index]

        df_ret["OrdStatus"] = "N"
        df_ret["ExecTransType"] = 2
        df_ret["IDSource"] = "RIC"
        df_ret["Trader"] = "DFLT"
        df_ret["Fund"] = "MCO_MST"
        df_ret["SettleCurrency"] = "USD"
        df_ret["PaymentFreq"] = 4
        df_ret["RateSource"] = df_csv['RIC Code'].apply(self.fill_ratesource)
        df_ret["PostCommAndFeesOnInit"] = "N"
        df_ret.loc[df_ret["Maturity Date"] == "NaT", "Maturity Date"] = None
        return df_ret

    def run(self):
        df_rpt = pd.DataFrame()
        for fpath in self.list_fpaths:
            list_splitted_fpath = fpath.split('_')
            broker_alias = list_splitted_fpath[1]
            if broker_alias == 'cicc':
                df_fmt_data = self.fmt_cicc(fpath)
            elif broker_alias == 'cs':
                df_fmt_data = self.fmt_cs(fpath)
            elif broker_alias == 'ms':
                df_fmt_data = self.fmt_ms(fpath)
            elif broker_alias == 'jpm':
                df_fmt_data = self.fmt_jpm(fpath)
            else:
                raise ValueError('Wrong broker alias in the file name.')
            df_rpt = df_rpt.append(df_fmt_data)
        df_rpt.to_csv(f'Mingshi_trade_{self.str_date}.csv', index=False)
        print('Format finished!')


if __name__ == '__main__':
    task = FmtTradeFilesFromDifferentBrokers('20200713', ['ms'])
    task.run()




