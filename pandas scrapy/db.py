'''
Created on 2018年8月18日
'''
import pymysql
    
class MariaDB():
    
    connection=pymysql.connect
    cursor=pymysql.cursors
    
    def __init__(self):
        pass
    
    def __del__(self):
        try:
            self.connection.close()
        except:
            pass
        
    def connect(self):
        self.connection = pymysql.connect(host='localhost',
                                          port=3306,
                                          user='root',
                                          passwd='lin89919',
                                          db='test',
                                          charset='utf8')
        self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        
    def executeone(self, sql):
        self.connect()
        try:
            self.cursor.execute(sql)
        except:
            print(sql)
            print('executeone() failed!!')
        finally:
            self.connection.close()
            
    def executemany(self, sqlList):
        self.connect()
        currentSql=""
        try:
            for sql in sqlList:
                currentSql=sql
                self.cursor.execute(sql)
            self.connection.commit()
        except:
            self.connection.rollback()
            print(currentSql)
            print('executemany() failed!!')
        finally:
            self.connection.close()
            
    def queryall(self, sql):
        self.connect()
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except:
            print(sql)
            print('queryall() failed!!')
        finally:
            self.connection.close()
            
    def querymany(self, sql, rows):
        self.connect()
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchmany(rows)
        except:
            print(sql)
            if rows==1:
                print('queryone() failed!!')
            else:
                print('querymany() failed!!')
        finally:
            self.connection.close()
        
    def queryone(self, sql):
        return self.querymany(sql, 1)
    
    def create_table(self, fieldList):
        self.connect()
        try:
            primaryKeyStr=''
            tableName=fieldList[0].table_name
            sql = ''
            for field in fieldList:
                if sql=='':
                    sql="create table {}({} {}".format(tableName,field.name,field.dbType)
                else:
                    sql += ',{} {}'.format(field.name,field.dbType)
                    
                if not field.allowNull:
                    sql += ' not null'
                    
                if 'char' in field.dbType:
                    sql += " default ''"
                else:
                    sql += ' default 0'
                    
                if field.isPrimarykey:
                    if primaryKeyStr=='':
                        primaryKeyStr = ' ,primary key({}'.format(field.name)
                    else:
                        primaryKeyStr += ',{}'.format(field.name)
            if primaryKeyStr!='':
                primaryKeyStr += ')'
            sql += primaryKeyStr+')'
            self.cursor.execute(sql)
        except:
            return '{} is exists'.format(tableName)
        finally:
            self.connection.close()
            
    def forceCreate_table(self, fieldList):
        if 'is exists'.format(fieldList[0].table_name) in self.create_table(fieldList):
            print("{} is exists".format(fieldList[0].table_name))
            self.connect()
            self.cursor.execute('drop table {}'.format(fieldList[0].table_name))
            self.create_table(fieldList)
            print("{} is created".format(fieldList[0].table_name))
        
    def dict2InsertSQL(self, dataDict):
        try:
            tableName = ''
            nameStr = ''
            valuesStr = ''
            for key in dataDict:
                value = dataDict[key]
                if key=='table_name':
                    tableName=value
                else:
                    if nameStr=='':
                        nameStr = "({}".format(key)
                    else:
                        nameStr += ",{}".format(key)
                    
                    if valuesStr=='':
                        if type(value)==str:
                            valuesStr = "('{}'".format(value)
                        else:
                            valuesStr = "({}".format(value)
                    else:
                        if type(value)==str:
                            valuesStr += ",'{}'".format(value)
                        else:
                            valuesStr += ",{}".format(value)
            if nameStr!='':
                nameStr += ')'
            if valuesStr!='':
                valuesStr += ')'
            if tableName!='':
                return "replace into {}{} values{}".format(tableName,nameStr,valuesStr)
            else:
                return ''
        except:
            return 'catch except'
    
    def fields2InsertSQL(self, fieldList):
        sql = ''
        if len(fieldList)==0:
            return sql
        tableName = fieldList[0].table_name
        valuesStr = ''
        for field in fieldList:
            if sql=='':
                sql = "replace into {}({}".format(tableName, field.name)
            else:
                sql += ",{}".format(field.name)
            if 'char' in field.dbType:
                if valuesStr=='':
                    valuesStr = " values('{}'".format(field.value)
                else:
                    valuesStr += ",'{}'".format(field.value)
            else:
                if valuesStr=='':
                    valuesStr = " values({}".format(field.value)
                else:
                    valuesStr = " ,{}".format(field.value)
        if valuesStr!='':
            valuesStr+=')'
        if sql!='':
            sql += "){}".format(valuesStr)
        
        return sql

    def fields2UpdateSQL(self, fieldList):
        sql = ''
        if len(fieldList)==0:
            return sql
        tableName = fieldList[0].table_name
        valuesStr = ''
        whereStr = ''
        for field in fieldList:
            if sql=='':
                sql = "update {}({}".format(tableName, field.name)
            else:
                sql += ",{}".format(field.name)
                
            if 'char' in field.dbType:
                if valuesStr=='':
                    valuesStr = " values('{}'".format(field.value)
                else:
                    valuesStr += ",'{}'".format(field.value)
            else:
                if valuesStr=='':
                    valuesStr = " values({}".format(field.value)
                else:
                    valuesStr = " ,{}".format(field.value)
            
            if field.isPrimarykey:
                if 'char' in field.dbType:
                    if whereStr=='':
                        whereStr = "where {}='{}'".format(field.name,field.value)
                    else:
                        whereStr += ",{}='{}'".format(field.name,field.value)
                else:
                    if whereStr=='':
                        whereStr = "where {}={}".format(field.name,field.value)
                    else:
                        whereStr += ",{}={}".format(field.name,field.value)
        if whereStr=='':
            return ''
           
        if valuesStr!='':
            valuesStr+=')'
        if sql!='':
            sql += "){} {}".format(valuesStr,whereStr)
        
        return sql

    def fields2DeleteSQL(self, fieldList):
        sql = ''
        if len(fieldList)==0:
            return sql
        tableName = fieldList[0].table_name
        whereStr = ''
        for field in fieldList:
            if field.isPrimarykey:
                if 'char' in field.dbType:
                    if whereStr=='':
                        whereStr = "where {}='{}'".format(field.name,field.value)
                    else:
                        whereStr += ",{}='{}'".format(field.name,field.value)
                else:
                    if whereStr=='':
                        whereStr = "where {}={}".format(field.name,field.value)
                    else:
                        whereStr += ",{}={}".format(field.name,field.value)
        if whereStr=='':
            return ''
        else:
            return "delete {} {}".format(tableName, whereStr)