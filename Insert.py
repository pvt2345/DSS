from Database.DataAccess import DataAccess
import pandas as pd

df = pd.read_csv("D:/20181/DecisionSupportSystems/Database/CafeF.HNX.Upto28.09.2018.csv")
df_ = pd.read_csv("D:/20181/DecisionSupportSystems/Database/CafeF.HSX.Upto28.09.2018.csv")
df__ = pd.read_csv("D:/20181/DecisionSupportSystems/Database/CafeF.UPCOM.Upto28.09.2018.csv")
# print(df)
#
# df[df['<Ticker>'] == 'AAV']
#
# for item in df['<DTYYYYMMDD>'][0:10]:
#     print(item)
#
# daylist = ['{}'.format(item) for item in df['<DTYYYYMMDD>']]
# daylist = list(set(daylist))
#
da = DataAccess()
# for item in daylist:
#     y = item[:4]
#     m = item[4:6]
#     d = item[6:]
#     da.ExecuteCommand('INSERT INTO dimdate (Day, Month, Year) VALUES ({}, {}, {})'.format(d, m, y))

# print(df['<Ticker>'])

# print(a.__len__())
a = list(set(df['<DTYYYYMMDD>'])) + list(set(df_['<DTYYYYMMDD>'])) + list(set(df__['<DTYYYYMMDD>']))
a = list(set(a))
for item in sorted(a):
# for item in daylist:
    item_ = str(item)
    y = item_[:4]
    m = item_[4:6]
    d = item_[6:]
    if(int(m) < 4):
        q = 1
    elif(int(m) < 7):
        q = 2
    elif(int(m) < 10):
        q = 3
    else:
        q = 4
    da.ExecuteCommand('INSERT INTO dimdate (date, month, quarter, year) VALUES ({}, {}, {}, {})'.format(d, m, q, y))


# for item in daylist:
#     y = item[:4]
#     m = item[4:6]
#     d = item[6:]

#     da.ExecuteCommand('INSERT INTO dimdate (Day, Month, Year) VALUES ({}, {}, {})'.format(d, m, y))
a = df.iloc[:100]
for item in a:
    item_ = item['<DTYYYYMMDD>']
    item_ = str(item_)
    y = item_[:4]
    m = item_[4:6]
    d = item_[6:]
    # if (int(m) < 4):
    #     q = 1
    # elif (int(m) < 7):
    #     q = 2
    # elif (int(m) < 10):
    #     q = 3
    # else:
    #     q = 4
    dateid = da.GetData('SELECT id FROM dimdate WHERE date = {0}, month = {1}, year = {2}'.format(d, m, y))
    dateid_ = dateid[0]['id']
