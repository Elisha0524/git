import pandas as pd
import db 
#import Field

tables = pd.read_html("http://www.stockq.org/funds/fidelity.php")
#sqlList=['delete from asia_fundation']
sqlList=[]
try:
    asia_funds = tables[11]
    for index, data in enumerate(asia_funds.values):
        if not pd.isnull(data[1]):
            sql=""" 
                REPLACE INTO asia_fundation (fundName, currency, netValue, upDown, percent, fundDate) 
                                     VALUES ('{}', '{}', '{}', '{}', '{}', '{}') 
                """.format(data[1],data[3],data[5],data[7],data[9],data[11])
            sqlList.append(sql)
except:
    print('table index out of range')
'''   
fields = []
fundName = Field.Field('fundName','asia_fundation')
fundName.isPrimarykey=True
fundName.dbType='varchar(50)'
fields.append(fundName)

currency = Field.Field('currency','asia_fundation')
currency.isPrimarykey=True
fields.append(currency)

netValue = Field.Field('netValue','asia_fundation')
fields.append(netValue)

upDown = Field.Field('upDown','asia_fundation')
fields.append(upDown)

percent = Field.Field('percent','asia_fundation')
fields.append(percent)

fundDate = Field.Field('fundDate','asia_fundation')
fundDate.isPrimarykey=True
fields.append(fundDate)
''' 
db = db.MariaDB()
#db.forceCreate_table(fields)
db.executemany(sqlList)
print('done')
