from datetime import datetime
import pandas as pd
import smtplib
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from WindPy import w


class GetAndSendRestrictedStocksList:
    def __init__(self):
        self.str_today = datetime.today().strftime('%Y%m%d')
        self.fn = f'Restricted List {self.str_today}.csv'
        w.start()

    @staticmethod
    def send_email(filename):
        smtpserver = 'smtp.exmail.qq.com'
        user = 'maxinzhe@mingshiim.com'  # 更改
        password = 'Ms123456'            # 更改
        receivers = ['wuhao@mingshiim.com']  # 更改
        msg = MIMEMultipart()
        subject = Header(filename, 'utf-8').encode()
        msg["Subject"] = subject
        msg["From"] = user
        msg["To"] = ','.join(receivers)
        msg.attach(MIMEText('The attachment is the restricted stocks list, please Check.', _subtype='html', _charset='utf-8'))
        part = MIMEApplication(open(filename, 'rb').read())
        part.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(part)
        try:
            s = smtplib.SMTP_SSL(smtpserver, port=465)
            s.login(user, password)
            s.sendmail(user, receivers, msg.as_string())
            s.close()
        except Exception as e:
            print("send email error:"+str(e))

    def get_restricted_stocks_list_csv(self):
        wset_all_stcodes = w.wset("sectorconstituent", f"date={self.str_today};sectorid=a001010100000000")
        list_all_stcodes = wset_all_stcodes.Data[1]
        str_all_stcodes = ','.join(list_all_stcodes)
        wss_all_stcodes_riskwarning = w.wss(str_all_stcodes, "riskwarning", f"tradeDate={self.str_today}")
        list_codes = wss_all_stcodes_riskwarning.Codes
        list_riskwarning = wss_all_stcodes_riskwarning.Data[0]
        df_riskwarning = pd.DataFrame({'WindCode': list_codes, 'RiskWarning': list_riskwarning})
        df_riskwarning_filtered = df_riskwarning[df_riskwarning['RiskWarning'] == '是'].copy()
        list_windcodes_riskwarning = df_riskwarning_filtered['WindCode'].to_list()
        df_csv_stock_code = pd.read_csv('Stock Code.csv')
        df_dict_ric2isin = df_csv_stock_code.loc[:, ['RIC', 'Short_Name', 'Ticker']].copy().set_index('RIC')
        dict_ric2isin = df_dict_ric2isin.to_dict()
        list_dicts_restricted_info = []
        for wcodes_restricted in list_windcodes_riskwarning:
            ric = wcodes_restricted.replace('.SH', '.SS')
            dict_restricted_info = {
                'Code': str(int(ric[:-3])),
                'Short_Name': dict_ric2isin['Short_Name'][ric],
                'ID_ISIN': dict_ric2isin['Ticker'][ric],
            }
            list_dicts_restricted_info.append(dict_restricted_info)

        df_restricted_info = pd.DataFrame(list_dicts_restricted_info)
        df_restricted_info.to_csv(f'Restricted List {self.str_today}.csv', index=False)

    def run(self):
        self.get_restricted_stocks_list_csv()
        self.send_email(self.fn)
        print('Done')


if __name__ == '__main__':
    task = GetAndSendRestrictedStocksList()
    task.run()

