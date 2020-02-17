'''
Created on 2018年8月18日

@author: ACER
'''

class Field():
    name=''
    value=''
    dbType='varchar(20)'
    table_name=''
    isPrimarykey=False
    allowNull=False

    def __init__(self, name, tableName):
        self.name=name
        self.table_name=tableName
        