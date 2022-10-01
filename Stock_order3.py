# Packages
print ("hello")
from calendar import day_name
from nsetools import Nse  # For bringing presnet day data
import pandas as pd  # for handling data set
from nsepy import get_history
from datetime import date, timedelta
# from datetime import datetime, timedelta
from datetime import datetime
import datetime as dt
import threading
# from openpyxl.workbook import Workbook
import time
import quandl
import datetime as dt
import traceback

from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib
import sys
import os



quandl.ApiConfig.api_key = "zaJzR_Rz4uY3vJWt3Jia"
nse = Nse()
from bsedata.bse import BSE

t1 = time.time()

# def check_daily():  # Gives information for given stock
#     q = nse.get_quote(row['Symbl'])
#
#     def check_per(row):
#         return (q['open'] - q['previousClose']) * 100 / q['open']
#
#     def check_per_max(row):
#         return (q['dayHigh'] - q['previousClose']) * 100 / q['dayHigh']
#
#     def check_per_min(row):
#         return (q['previousClose'] - q['dayLow']) * 100 / q['previousClose']
#
#     df = pd.read_csv('1st.csv')
#
#     df['Change'] = df.apply(lambda row: check_per(row), axis=1)
#     df['Max_Change'] = df.apply(lambda row: check_per_max(row), axis=1)
#     df['Min_Change'] = df.apply(lambda row: check_per_min(row), axis=1)

stock_name_list = []
target_sell_percentage_up_list = []
target_sell_percentage_down_list = []
non_trading_days_list = []
stk_list_rmv = []
last_closing_price = []
stock_exchange = 'NSE'
pr = []
# def profit_stocks(data):
#     global pr
#     data = data.head(2)[['Symbl', 'ClosingPrice','Per Change High','Per Change Low','Flag']]
#     for (i,j)in zip(data['ClosingPrice'].values, data['Flag'].values):
#         if j == 1:
#             pr.append((1.14*i,i))
#         elif j == 0:
#             pr.append((0.97*i,i))
#         else :
#             pr.append((0,i))
    


    


if stock_exchange == 'NSE':
    NSE_df = pd.read_csv('consolidate_NSE_Stock.csv')
    # NSE_df = pd.read_csv('/home/akashthakur/Desktop/Data/Data/Desktop_Temp/SS/SS/Traded_non_info/consolidate_NSE_Stock.csv')
  #  NSE_NON_df = pd.read_csv('Traded_non_info/NSE_Non_traded.csv')
    stk_list = NSE_df['Stock_symbl']
    #stk_list = stk_list[:100]
    # print (len(stk_list))
    # stk_Non_list = NSE_NON_df['Stock_symbl']
    # stk_list = list(set(stk_list) - set(stk_Non_list))

else:
    with open('BSE_data.txt') as f:
        content = f.readlines()
    # you may also want to remove whitespace characters like `\n` at the end of each line
    content = [x.strip() for x in content]

    content = [c.split(' ')[-1].split('|')[-1] for c in content]
    content = ["BSE/" + c for c in content]
    stk_list = content
    #stk_list = stk_list[:300]

def send_email(subject, df_test = None, df_test2 = None):
    recipients = ['guglu000@gmail.com']
    emaillist = [elem.strip().split(',') for elem in recipients]
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = 'sakshamtest2002@gmail.com'
    # if  df_test2 != None:
    html = """\
            <html>
              <head></head>
              <body>
              <p>Stocks and their purchase price</p>
                {0}
               <p>Complete list of shortlisted stocks</p>
               {1}
              </body>
            </html>
            """.format(df_test.to_html(), df_test2.to_html())
    # if df_test2 == None:
    #     html = """\
    #     <html>
    #       <head></head>
    #       <body>
    #       <p>Saturated Stocks to purchase</p>
    #         {0}
    #       </body>
    #     </html>
    #     """.format(df_test.to_html())
    # if df_test2 == df_test == None:
    #     html = """\
    #             <html>
    #               <head></head>
    #               <body>
    #               <p>No stock got shortlisted in the given condition</p>
    #               </body>
    #             </html>
    #             """

    part1 = MIMEText(html, 'html')
    msg.attach(part1)

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()

    # Credentials
    server.login("sakshamtest2002@gmail.com", "C@mel1990")
    server.sendmail(msg['From'], emaillist, msg.as_string())


def today_price_change(stock_symbl):
    if stock_exchange == 'NSE':
        nse = Nse()
        q = nse.get_quote(stock_symbl)
        # print (q)
        open_price = q['open']
        current_price = q['lastPrice']
        if ((q['high52'] - current_price) / current_price) * 100 < 3 and ((q['high52'] - current_price) / current_price) * 100 > -3 :
            return 0
        if ((q['high52'] - current_price) / current_price) * 100 < 3 and ((q['high52'] - current_price) / current_price) * 100 > -3 :
            return 0
        return ((current_price - open_price) / open_price) * 100
    else:
        b = BSE()
        stock_id = stock_symbl.split('/')[1]
        stock_id = stock_id.replace('BOM', "")
        q = b.getQuote(stock_id)
        if ((float(q['52weekHigh']) - float(q['currentValue'])) / float(q['currentValue'])) * 100 < 3 and ((float(q['52weekHigh']) - float(q['currentValue'])) / float(q['currentValue'])) * 100 > -3 :
            return 0
        if ((float(q['52weekHigh']) - float(q['currentValue'])) / float(q['currentValue'])) * 100 < 3 and ((float(q['52weekHigh']) - float(q['currentValue'])) / float(q['currentValue'])) * 100 > -3 :
            return 0
        return ((float(q['currentValue']) - float(q['previousClose']))/ float(q['previousClose'])) * 100


# today_price_change('GSPL')
# sys.exit()
# Read Stock data
# May add BSE later on


def get_history_BSE(stock_name, start, end):
    data = quandl.get(stock_name, start_date=start, end_date=end)
    data['Prev Close'] = data.Close.shift(1)
    return data


def get_data (stock_name,date_detail = datetime.today()):
    date_detail1 = date_detail.strftime('%Y-%m-%d')
    e_y = int(date_detail1.split('-')[0])
    e_m = int(date_detail1.split('-')[1])
    e_d = int(date_detail1.split('-')[2])
    date_detail = (date_detail - dt.timedelta(days=15)).date().strftime('%Y-%m-%d')

    s_y = int(date_detail.split('-')[0])
    s_m = int(date_detail.split('-')[1])
    s_d = int(date_detail.split('-')[2])
    
    return get_history(symbol=stock_name, start=date(s_y, s_m, s_d), end=date(e_y, e_m, e_d))
    print(data_ret(date_detail=(datetime.today() - dt.timedelta(days=15))))


def daily_Per_change(data):
    data['change_per_in_day'] = (data['High'] - data['Open']) * 100 / data['High']
    data['change_per_over_day'] = (data['Open'] - data['Prev Close']) * 100 / data['Open']
    data['Pos_change_per_over_in_day'] = (data['High'] - data['Prev Close']) * 100 / data['High']
    data['Neg_change_per_over_in_day'] = (data['Prev Close'] - data['Low']) * 100 / data['Prev Close']
    data['Date'] = data.index
    data['Date'] = data['Date'].astype('datetime64[ns]')
    data['week_day'] = data['Date'].dt.day_name()
    data = data[['change_per_over_day', 'change_per_in_day', 'Pos_change_per_over_in_day', 'Neg_change_per_over_in_day',
                 'week_day', 'Open','Prev Close']]
    return data


def momentum(data):
    data2 = data.tail(3)
    df_change_list = []

    def df_change(data2, i):
        # print (data2['High'][i])
        df_change_list.append((data2['High'][i] - data2['Close'][i]) * 100 / data2['High'][i])

    for i in range(3):
        df_change(data2, i)
    return df_change_list

    # #     if d2_change - d3_change >= 0:
    # #         if d1_change - d2_change >= 0:
    # #             print (data['Symbol'][0])
    #
    # return (d1_change, d2_change, d3_change)


# #Identify saturated stocks

def saturated_stocks():
    stock_list = []

    def sub_function(k):
        data = get_data(k)
        try:
            x = momentum(data)
            # if x[0] == x[1] == x[2] == x[3] == x[4] ==x[5] == x[6] == x[7] == x[8] == x[9]== 0:
            if x[0] == x[1] == x[2] == 0:
                # if today_price_change(data) == 0:
                    if data['Open'][-1] > 15:
                        if data['Open'][-1] > data['Open'][-2]:
                            print(k)
                            stock_list.append(k)
        except Exception:
            print("In except", k)
            # traceback.print_exc()
            stk_list_rmv.append(k)
            pass

    i = 0
    while i + 20 <= len(stk_list):
        print(i)
        t1 = threading.Thread(target=sub_function, args=(stk_list[i],))
        t2 = threading.Thread(target=sub_function, args=(stk_list[i + 1],))
        t3 = threading.Thread(target=sub_function, args=(stk_list[i + 2],))
        t4 = threading.Thread(target=sub_function, args=(stk_list[i + 3],))
        t5 = threading.Thread(target=sub_function, args=(stk_list[i + 4],))
        t6 = threading.Thread(target=sub_function, args=(stk_list[i + 19],))
        t7 = threading.Thread(target=sub_function, args=(stk_list[i + 5],))
        t8 = threading.Thread(target=sub_function, args=(stk_list[i + 6],))
        t9 = threading.Thread(target=sub_function, args=(stk_list[i + 7],))
        t10 = threading.Thread(target=sub_function, args=(stk_list[i + 8],))
        t11 = threading.Thread(target=sub_function, args=(stk_list[i + 9],))
        t12 = threading.Thread(target=sub_function, args=(stk_list[i + 10],))
        t13 = threading.Thread(target=sub_function, args=(stk_list[i + 11],))
        t14 = threading.Thread(target=sub_function, args=(stk_list[i + 12],))
        t15 = threading.Thread(target=sub_function, args=(stk_list[i + 13],))
        t16 = threading.Thread(target=sub_function, args=(stk_list[i + 14],))
        t17 = threading.Thread(target=sub_function, args=(stk_list[i + 15],))
        t18 = threading.Thread(target=sub_function, args=(stk_list[i + 16],))
        t19 = threading.Thread(target=sub_function, args=(stk_list[i + 17],))
        t20 = threading.Thread(target=sub_function, args=(stk_list[i + 18],))

        t1.start()
        t2.start()
        t3.start()
        t4.start()
        t5.start()
        t6.start()
        t7.start()
        t8.start()
        t9.start()
        t10.start()
        t11.start()
        t12.start()
        t13.start()
        t14.start()
        t15.start()
        t16.start()
        t17.start()
        t18.start()
        t19.start()
        t20.start()
        t1.join()
        t2.join()
        t3.join()
        t4.join()
        t5.join()
        t6.join()
        t7.join()
        t8.join()
        t9.join()
        t10.join()
        t11.join()
        t12.join()
        t13.join()
        t14.join()
        t15.join()
        t16.join()
        t17.join()
        t18.join()
        t19.join()
        t20.join()

        i += 20

    for k in range(i + 1, len(stk_list)):
        print(k)
        tn = threading.Thread(target=sub_function, args=(stk_list[k],))
        tn.start()
        tn.join()
    return stock_list


def stock_shortlist(data):
    if len(data) < 5:
        return
    data = data.tail(5)
    target_sell_percentage_up = 0.8 * data['Pos_change_per_over_in_day'].tail(3).mean() + 0.2* data[
                                                                                                    'Pos_change_per_over_in_day'][
                                                                                                3:5].mean() + 0* \
                                data['Pos_change_per_over_in_day'][3:5].mean()
    target_sell_percentage_down = 0.8 * data['Neg_change_per_over_in_day'].tail(3).mean() + 0.2 * data[
                                                                                                      'Neg_change_per_over_in_day'][
                                                                                                  3:5].mean() + 0* \
                                  data['Neg_change_per_over_in_day'][3:5].mean()
    if target_sell_percentage_up - target_sell_percentage_down > 1.5:
        dx = data.groupby(['week_day'], as_index=False).mean()['Pos_change_per_over_in_day'].idxmin()
        non_trading_days = data.groupby(['week_day'], as_index=False).mean().loc[dx]['week_day']
        last_closing_price = data['Prev Close'][-1]
        return (target_sell_percentage_up, target_sell_percentage_down, non_trading_days, last_closing_price)
    else:
        pass

def next_day(symbl,date_detail):
    
    x =1
    if(date_detail.weekday()==4):
        x = 3
    date_detail1 = (date_detail + dt.timedelta(days=x)).date().strftime('%Y-%m-%d')
    
    s_y = int(date_detail1.split('-')[0])
    s_m = int(date_detail1.split('-')[1])
    s_d = int(date_detail1.split('-')[2])
    date2 = (date_detail + dt.timedelta(days=x)).date().strftime('%Y-%m-%d')

    e_y = int(date2.split('-')[0])
    e_m = int(date2.split('-')[1])
    e_d = int(date2.split('-')[2])
    df = get_history(symbol=symbl, start=date(s_y, s_m, s_d), end=date(e_y, e_m, e_d),index=False)
    try:

        high = [df["High"].values[0]]
        low = [df["Low"].values[0]]
        close = df["Prev Close"].values
        per_Hchange_on_prevClose = [(high[0]-close[0])/close[0]*100]
        per_Lchange_on_prevClose = [(low[0]-close[0])/close[0]*100]
        flag = [-1]
        if per_Hchange_on_prevClose[0]>1.4:
            flag[0]=1
        elif per_Lchange_on_prevClose[0] < -3:
            flag[0]=0
        return [high,low,per_Hchange_on_prevClose,per_Lchange_on_prevClose,flag]
    except IndexError as e:
        print(e)


    
def master(stock_name,date):
    global stock_name_list
    global target_sell_percentage_up_list
    global target_sell_percentage_down_list
    global non_trading_days_list
    global last_closing_price
    global stk_list_rmv
    data = get_data(stock_name,date)
    try:
        if data['Open'][-1] < 50:
            return
    except:
        pass
    data = daily_Per_change(data)
    if stock_shortlist(data) is not None:
        if len(data) >= 5:
            if today_price_change(stock_name) > 1:
                x = stock_shortlist(data)
                stock_name_list.append(stock_name)
                target_sell_percentage_up_list.append(x[0])
                target_sell_percentage_down_list.append(x[1])
                non_trading_days_list.append(x[2])
                last_closing_price.append(x[3])
    else:
        # print (stock_name)
        stk_list_rmv.append(stock_name)

def Final_list(Stock_name,date):
    i = 0
    if (stock_exchange == 'NSE') or (stock_exchange == 'BSE'):
        while i + 20 <= len(Stock_name):
            print(i)
            t1 = threading.Thread(target=master, args=(Stock_name[i],date))
            t2 = threading.Thread(target=master, args=(Stock_name[i + 1],date))
            t3 = threading.Thread(target=master, args=(Stock_name[i + 2],date))
            t4 = threading.Thread(target=master, args=(Stock_name[i + 3],date))
            t5 = threading.Thread(target=master, args=(Stock_name[i + 4],date))
            t6 = threading.Thread(target=master, args=(Stock_name[i + 19],date))
            t7 = threading.Thread(target=master, args=(Stock_name[i + 5],date))
            t8 = threading.Thread(target=master, args=(Stock_name[i + 6],date))
            t9 = threading.Thread(target=master, args=(Stock_name[i + 7],date))
            t10 = threading.Thread(target=master, args=(Stock_name[i + 8],date))
            t11 = threading.Thread(target=master, args=(Stock_name[i + 9],date))
            t12 = threading.Thread(target=master, args=(Stock_name[i + 10],date))
            t13 = threading.Thread(target=master, args=(Stock_name[i + 11],date))
            t14 = threading.Thread(target=master, args=(Stock_name[i + 12],date))
            t15 = threading.Thread(target=master, args=(Stock_name[i + 13],date))
            t16 = threading.Thread(target=master, args=(Stock_name[i + 14],date))
            t17 = threading.Thread(target=master, args=(Stock_name[i + 15],date))
            t18 = threading.Thread(target=master, args=(Stock_name[i + 16],date))
            t19 = threading.Thread(target=master, args=(Stock_name[i + 17],date))
            t20 = threading.Thread(target=master, args=(Stock_name[i + 18],date))

            t1.start()
            t2.start()
            t3.start()
            t4.start()
            t5.start()
            t6.start()
            t7.start()
            t8.start()
            t9.start()
            t10.start()
            t11.start()
            t12.start()
            t13.start()
            t14.start()
            t15.start()
            t16.start()
            t17.start()
            t18.start()
            t19.start()
            t20.start()
            t1.join()
            t2.join()
            t3.join()
            t4.join()
            t5.join()
            t6.join()
            t7.join()
            t8.join()
            t9.join()
            t10.join()
            t11.join()
            t12.join()
            t13.join()
            t14.join()
            t15.join()
            t16.join()
            t17.join()
            t18.join()
            t19.join()
            t20.join()

            i += 20
    else:
        while i + 5 <= len(Stock_name):
            print(i)
            t1 = threading.Thread(target=master, args=(Stock_name[i],date))
            t2 = threading.Thread(target=master, args=(Stock_name[i + 1],date))
            t3 = threading.Thread(target=master, args=(Stock_name[i + 2],date))
            t4 = threading.Thread(target=master, args=(Stock_name[i + 3],date))
            t5 = threading.Thread(target=master, args=(Stock_name[i + 4],date))

            t1.start()
            t2.start()
            t3.start()
            t4.start()
            t5.start()
            t1.join()
            t2.join()
            t3.join()
            t4.join()
            t5.join()
            i += 5

    for k in range(i + 1, len(Stock_name)):
        print(k)
        t1 = threading.Thread(target=master, args=(Stock_name[k],date))
        t1.start()
        t1.join()
    print (stock_name_list)
    final_df = pd.DataFrame(
        {'Symbl': stock_name_list, 'UP': target_sell_percentage_up_list, 'LP': target_sell_percentage_down_list,
         'NTD': non_trading_days_list,'ClosingPrice': last_closing_price})

    final_df['Change'] = final_df['UP'] - final_df['LP']
    # final_df['Ratio'] = final_df['UP']/final_df['LP']
    final_df = final_df.sort_values(['LP'], ascending=[False])
    ls = [[],[],[],[],[]]
    
    for i in final_df["Symbl"].values:
        #function here
        l = next_day(i,date)
        ls[0].extend(l[0])
        ls[1].extend(l[1])
        ls[2].extend(l[2])
        ls[3].extend(l[3])
        ls[4].extend(l[4])
    final_df['Next Day High'] = ls[0]
    final_df['Next Day Low'] = ls[1]
    final_df['Per Change High'] = ls[2]
    final_df['Per Change Low'] = ls[3]
    final_df['Flag'] = ls[4]
    final_df = final_df.sort_values(['Per Change High'], ascending=[False])
    
    # profit_stocks(final_df)

    if stock_exchange == 'NSE':
        final_df.to_csv('NSE/' + date.strftime('%Y-%m-%d') + '.csv', index=False)
    else:
       pass
       # final_df.to_csv('BSE/' + datetime.now().strftime('%Y-%m-%d') + '.csv', index=False)
    if datetime.now().strftime("%A") != 'Friday':
        final_df = final_df[(final_df['NTD'] != (datetime.now() + timedelta(days=1)).strftime("%A"))]
    else:
        final_df = final_df[(final_df['NTD'] != (datetime.now() + timedelta(days=3)).strftime("%A"))]
    suggested_stock_to_purchase = final_df.head(5)[['Symbl', 'ClosingPrice','Next Day High',
    'Next Day Low','Per Change High','Per Change Low','Flag']]
    return suggested_stock_to_purchase, final_df


def write_file_list_to_csv(file_name, input_list_name):
    df = pd.DataFrame({'Stock_symbl': input_list_name})
    df.to_csv(file_name, index=False)
    # with open(file_name, "w") as fobj:
    #     for x in input_list_name:
    #         fobj.write(str(x) + "\n")
    # fobj.close()


def suggested_stock_to_purchase(date):
    suggested_stock_to_purchase, final_df = Final_list(stk_list,date)
    if stock_exchange == 'NSE':
        # write_file_list_to_csv('NSE/' + datetime.now().strftime('%Y-%m-%d') + ".csv", suggested_stock_to_purchase)
        suggested_stock_to_purchase.to_csv('NSE/' + date.strftime('%Y-%m-%d') + "Rec"+".csv", index = False)

    #else:
        # write_file_list_to_csv('BSE/' + datetime.now().strftime('%Y-%m-%d') + ".csv", suggested_stock_to_purchase)
        #suggested_stock_to_purchase.to_csv('NSE/' + datetime.now().strftime('%Y-%m-%d') + ".csv", index = False)

    print("Total Run Time", str(time.time() - t1))
    suggested_stock_to_purchase = suggested_stock_to_purchase.sort_values(['ClosingPrice'], ascending=[True])
    # send_email('Suggested Stock to Purchase', suggested_stock_to_purchase, final_df)
    # print(suggested_stock_to_purchase)


def saturated_stocks_to_purchase():
    saturated_stock_to_purchase = saturated_stocks()
    SSP_df = pd.DataFrame({'StockName': saturated_stock_to_purchase})
    if stock_exchange == 'NSE':
        write_file_list_to_csv('NSE/' + datetime.now().strftime('%Y-%m-%d') + "_ss.csv", saturated_stock_to_purchase)
    else:
        write_file_list_to_csv('BSE/' + datetime.now().strftime('%Y-%m-%d') + "_ss.csv", saturated_stock_to_purchase)
    #send_email("Upper Limit Stocks to Purchase", SSP_df, SSP_df)

    # if stock_exchange == 'BSE':
    #     write_file_list_to_csv('Traded_non_info/BSE_Non_traded.csv', stk_list_rmv)
    # else:
    #     write_file_list_to_csv('Traded_non_info/NSE_Non_traded.csv', stk_list_rmv)
    # print (saturated_stock_to_purchase)


# saturated_stocks_to_purchase()
c=2
d = 33
while c < d:
    
    dte = (datetime.today() - dt.timedelta(days=c))
    d_name = dte.strftime('%Y-%m-%d')
    if dte.weekday() == 6 or dte.weekday() ==5 or d_name == "2022-03-01":
        c = c+1
        d = d+1
        continue

    print(d_name)
    suggested_stock_to_purchase(dte)
    c = c+1
# invested= 0
# sold = 0
# unsold = 0
# for (i,j) in pr:
#     invested = invested +j
#     sold = sold + i
#     if i == 0:
#         unsold = unsold+ j
# p = sold-invested
# print("Total Invested  :  ",invested,"\n","Total Unsold  :  ",unsold,"\n","Total Profit  :  ",p)
    
    
    
    
    


# print (stk_list_rmv)
write_file_list_to_csv('NSE_Non_traded.csv', stk_list_rmv)
sys.exit()


# ToDO:
# Solve saturated stocks issues - solved
# Write logic for GTT regular and GTT UL stocks
# Early Execution: Include stocks that are going to hit upper limit(Think of a logic to identify upper limit
# Make code ready for github release - Done
# Check for the missing stocks from NSE and BSE
# Function to update the stock list on a daily basis
#Get alternate function to capute data for NSE traded stocks not getting captured currently
#Get an email module
#Get a module to suggest price

#Insights
#Purchase UL stocks only after it remains high for atleast 2 days