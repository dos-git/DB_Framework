
import os, sys
import sqlite3

NOERROR = 0
ERROR_TABLE_EXIST = 20
ERROR_TABLE_CORRUPT = 21

class Database(object):

    def __init__(self, db_name=None, db_path=None, db_table_struct=None):

        self.db_name = db_name
        self.db_path = os.getcwd() + os.sep + db_path

        if db_table_struct == None:

            self.db_table_name = ""
            self.column = 0
            self.row = 0
            self.items = ()
            self.keys = ()
            self.sql_query_create = ""
        else:
            #get_model - retrieve fields that describe db model
            self.db_table_name = db_table_struct["name"][0]
            self.keys = db_table_struct["pk"]
            self.items = tuple(item[0] for item in db_table_struct["items"])

            self.row = len(self.items)
            self.column = len(db_table_struct)

            #print "".join()

            sub_sql_query = ", ".join([ " ".join(x) for x in MainDb._table_struct["items"]])
            sql_query_pk = "PRIMARY KEY ( %s )" % ", ".join(MainDb._table_struct["pk"])

            self.sql_query_create = "CREATE TABLE %s ( %s, %s );" % ( self.db_table_name, sub_sql_query, sql_query_pk)



    def Check_Db_Structure(self):

        sql_get_structure = "select sql from sqlite_master where type = 'table' " + \
                            "and name = \'%s\';" % ( self.db_table_name )
        (rc, data) = self.Execute(sql_get_structure)

        db_structure =  str(data[0][0] + ";")
        if db_structure == self.sql_query_create:
            print "The table was created"

        else:

            print db_structure
            print self.sql_query_create
            print "inne bazy"

        #print sql_get_structure
        #print self.sql_query_create

    def CreateTable(self):

        self.Execute(self.sql_query_create,values=())


    def ParseError(self, err_msg):

        err_msg_table_exist = "table %s already exists" % self.db_table_name

        if err_msg == err_msg_table_exist:
            print "juz jest"
            self.Check_Db_Structure()
        else:
            print "[%s]" %err_msg
            #print str_2_compare
            print "dasda"
            pass


    def Execute(self, sql_query, values=(), commit=False):

        rc = 0
        data = ()
        db_connection = None

        if os.path.exists(self.db_path):
            try:

                db_connection = sqlite3.connect(self.db_path)
                db_connection.text_factory = str
                db_cursor = db_connection.cursor()
                db_cursor.execute(sql_query, values)

                if commit == True:
                    db_connection.commit()
                else:
                    data = db_cursor.fetchall()
                    print "DATA: %s" % data

                if len(data) == 0:
                    data = ()

                rc = 0

            except sqlite3.Error as e:

                err_msg = str(e)
                self.ParseError(err_msg)
                rc = -1

            finally:
                if db_connection:
                    db_connection.close()

        else:
            print "Database %s does not exist." % self.db_path
            rc = -1

        return (rc, data)


    def AddRecord(self, key_values):

        sql_query_insert = "Insert INTO %s VALUES (" % self.db_table_name
        sql_sub_query = ""

        if len(key_values) == self.column:
            for index, element in enumerate(key_values):

                sql_sub_query += " ?"
                if index < self.column - 1:
                    sql_sub_query += ", "

            sql_query_insert += sql_sub_query + " );"

            self.Execute(sql_query_insert, key_values, True)
        else:
            print "Not enough values"

    def DeleteRecord(self, values):

        sql_query_delete = "DELETE FROM %s WHERE " % self.db_table_name

        if len(values) == len(self.keys):

            for index, element in enumerate(values):
                if index == 0:
                    sql_query_delete += "%s=?" % self.keys[index]
                else:
                    sql_query_delete += " and %s=?" % self.keys[index]
            sql_query_delete += ";"

            self.Execute(sql_query_delete, values, commit=True)

        else:
            print "Type more details."

    def ReadAll(self):

        sql_query_select = "SELECT * FROM %s;" % self.db_table_name
        (rc, data) = self.Execute(sql_query_select, commit=False)

        return (rc, data)

    def ReadRecord(self, values):

        rc = 0
        data = ()
        sql_query_select = "SELECT * FROM %s " % self.db_table_name

        if len(values) > 0:

            sql_query_select += "WHERE "
            for index, element in enumerate(values):
                if index == 0:
                    sql_query_select += "%s = ?" % self.keys[index]
                else:
                    sql_query_select += " and %s = ?" % self.keys[index]

            sql_query_select += ";"

            (rc, data) = self.Execute(sql_query_select, values)

            if rc != 0:
                print "Error, cannot read fro database."
        else:
            print "Type more details."

        return (rc, data)

    def UpdateRecord(self, values, conditions):

        rc = 0
        data = ()
        val = []
        sql_query_update = "UPDATE %s SET " % self.db_table_name

        for index, key in enumerate(values):

            if index == 0:
                sql_query_update += "%s=?" % key
            else:
                sql_query_update += ", %s=?" % key
            val.append(values[key])

        sql_condition = " WHERE "
        for index, key in enumerate(conditions):
            print key
            if index == 0:
                sql_condition += "%s=?" % key
            else:
                sql_condition += " AND %s=?" % key
            val.append(conditions[key])
        sql_condition += ";"

        sql_query_update += sql_condition
        val_tup = tuple(val)
        self.Execute(sql_query_update, val_tup, commit=True)


class MainDb(Database):

    _table_struct = {
        "name"  :   ("main_table",),
        "pk"    :   ("field_name", "id"),
        "items" :   (
                    ( "field_name",     "TEXT",     "NOT NULL", "DEFAULT \"\"" ),
                    ( "description",    "TEXT",     "NOT NULL", "DEFAULT \"\"" ),
                    ( "id",             "INTEGER",  "NOT NULL", "DEFAULT 0" )
        )
    }



    def __init__(self, db_name=None, db_path=None, db_table_struct=_table_struct):
        super(MainDb,self).__init__(db_name, db_path, db_table_struct)

    '''
    def __init__(self, db_name=None, db_path=None, db_table_struct=_table_struct):
        super(MainDb,self).__init__(db_name, db_table_struct)
    '''




if __name__ == "__main__":

    m = MainDb(db_path="db.db")
    m.CreateTable()
    m.AddRecord(("domino", "haha", 9))
    m.AddRecord(("domino", "haha", 9))

    c = { 'field_name' : "domino"}
    v = { 'field_name' : "changed", "description" : "something new", "id" : 1 }

    m.UpdateRecord(v, c)

    #m.Check_Db_Structure()
    '''
    .AddRecord(("domino", "haha", 9))
    #print m.ReadRecord(("domino", 9))[1]
    #m.DeleteRecord(("domino", 9))
    #print m.ReadRecord(("domino", 9))[1]
    #m.AddRecord(("domino", "haha", 9))
    m.AddRecord(("stefano", "haha", 9))
    m.AddRecord(("stefano", "haha", 11))
    print m.ReadAll()[1]
    '''




