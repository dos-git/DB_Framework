
import os, sys
import sqlite3

NOERROR = 0
ERROR_TABLE_NOT_EXISTS = 20
ERROR_TABLE_DIFF = 21
ERROR_SQL_QUERY = 22

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

            sub_sql_query = ", ".join([ " ".join(x) for x in db_table_struct["items"]])
            sql_query_pk = "PRIMARY KEY ( %s )" % ", ".join(db_table_struct["pk"])

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
        return 0, "", ()

    def add_record(self, key_values):

        rc = NOERROR; rm = ""
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
            rm = "Not enough values to add record in database"
            rc = ERROR_SQL_QUERY

        return rc, rm

    def delete_record(self, values):

        rc = NOERROR; rm = ""
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
            rm = "Not enough values to delete record in database"
            rc = ERROR_SQL_QUERY

        return rc, rm


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
            rm = "Not enough values to read record from database"
            rc = ERROR_SQL_QUERY

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



if __name__ == "__main__":
    pass