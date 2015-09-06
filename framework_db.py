
import os, sys
import sqlite3

NOERROR = 0
ERROR_TABLE_NOT_EXISTS = 20
ERROR_TABLE_DIFF = 21

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

            sub_sql_query = ", ".join([ " ".join(x) for x in MainDb._table_struct["items"]])
            sql_query_pk = "PRIMARY KEY ( %s )" % ", ".join(MainDb._table_struct["pk"])

            self.sql_query_create = "CREATE TABLE %s ( %s, %s );" % ( self.db_table_name, sub_sql_query, sql_query_pk)


    def check_structure(self):

        rc = 0; rm = ""

        sql_get_structure = "select sql from sqlite_master where type = 'table' " + \
                            "and name = \'%s\';" % ( self.db_table_name )
        rc, rm, data = self.execute(sql_get_structure)

        db_structure =  str(data[0][0] + ";")
        if db_structure == self.sql_query_create:
            rc = NOERROR
        else:
            rc = ERROR_TABLE_DIFF
            rm = "Table definition differs - structure [%s]" % sql_get_structure

        return rc, rm



    def create_table(self):

        rc, rm, data = self.execute(self.sql_query_create,values=())
        return rc, rm, data


    def parse_error(self, error_code, error_msg):

        rc = 0; rm = ""
        err_msg_table_exist = "table %s already exists" % self.db_table_name

        if error_msg == err_msg_table_exist:
            rc, rm = self.check_structure()
        else:
            rc = error_code
            rm = error_msg

        return rc, rm


    def execute(self, sql_query, values=(), commit=False):

        rc = 0; rm = ""; data = ()
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

                if len(data) == 0:
                    data = ()

            except sqlite3.Error as e:

                err_msg = str(e)
                rc, rm = self.parse_error(err_msg)

            finally:
                if db_connection:
                    db_connection.close()

        else:
            rm =  "Database %s does not exist." % self.db_path
            rc = ERROR_TABLE_NOT_EXISTS

        return rc, rm, data


    def add_record(self, key_values):

        sql_query_insert = "Insert INTO %s VALUES (" % self.db_table_name
        sql_sub_query = ""

        if len(key_values) == self.column:
            for index, element in enumerate(key_values):

                sql_sub_query += " ?"
                if index < self.column - 1:
                    sql_sub_query += ", "

            sql_query_insert += sql_sub_query + " );"

            self.execute(sql_query_insert, key_values, True)
        else:
            print "Not enough values"

    def delete_record(self, values):

        sql_query_delete = "DELETE FROM %s WHERE " % self.db_table_name

        if len(values) == len(self.keys):

            for index, element in enumerate(values):
                if index == 0:
                    sql_query_delete += "%s=?" % self.keys[index]
                else:
                    sql_query_delete += " and %s=?" % self.keys[index]
            sql_query_delete += ";"

            self.execute(sql_query_delete, values, commit=True)

        else:
            print "Type more details."

    def read_all(self):

        sql_query_select = "SELECT * FROM %s;" % self.db_table_name
        rc, rm, data = self.execute(sql_query_select, commit=False)
        return rc, rm, data

    def read_record(self, values):

        rc = 0; rm =""
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

            rc, rm, data = self.execute(sql_query_select, values)

            if rc != 0:
                rm = "Error - cannot read from database [%s]." % self.db_name
        else:
            pass

        return rc, rm, data

    def update_record(self, values, conditions):

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

            if index == 0:
                sql_condition += "%s=?" % key
            else:
                sql_condition += " AND %s=?" % key
            val.append(conditions[key])
        sql_condition += ";"

        sql_query_update += sql_condition
        val_tup = tuple(val)
        rc, rm, data = self.execute(sql_query_update, val_tup, commit=True)
        return rc, rm


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
    m.create_table()
    m.add_record(("domino", "haha", 9))
    m.add_record(("domino", "haha", 9))

    c = { 'field_name' : "domino"}
    v = { 'field_name' : "changed", "description" : "something new", "id" : 1 }

    m.update_record(v, c)

    #m.check_structure()
    '''
    .add_record(("domino", "haha", 9))
    #print m.read_record(("domino", 9))[1]
    #m.delete_record(("domino", 9))
    #print m.read_record(("domino", 9))[1]
    #m.add_record(("domino", "haha", 9))
    m.add_record(("stefano", "haha", 9))
    m.add_record(("stefano", "haha", 11))
    print m.read_all()[1]
    '''




