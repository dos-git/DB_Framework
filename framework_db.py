
import os, sys
import sqlite3

NOERROR = 0
ERROR_TABLE_NOT_EXISTS = 20
ERROR_TABLE_DIFF = 21
ERROR_SQL_QUERY_VALUES = 22
ERROR_ITEM_NOT_UNIQUE = 25

class Database(object):

    def __init__(self, db_name="", db_path="", db_table_struct=None):

        self.db_name = db_name
        self.db_path = os.getcwd() + os.sep + db_path

        if db_table_struct == None :

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
            #CREATE TABLE main_table ( field_name TEXT NOT NULL DEFAULT "", id INTEGER NOT NULL DEFAULT 0, PRIMARY KEY ( field_name, id ) );

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

    def add_record(self, record):

        rc = NOERROR
        rm = ""
        data = ()
        sql_query_insert = "INSERT INTO %s VALUES (" % self.db_table_name
        sql_sub_query = ""

        if len(record) == self.column:
            for index, element in enumerate(record):

                sql_sub_query += " ?"
                if index < self.column - 1:
                    sql_sub_query += ", "

            sql_query_insert += sql_sub_query + " );"

            rc, rm, data = self.execute(sql_query_insert, record, True)
        else:
            rm = "Wrong amount of values"
            rc = ERROR_SQL_QUERY_VALUES

        return rc, rm

    def delete_record(self, record):

        rc = NOERROR; rm = ""
        sql_query_delete = "DELETE FROM %s WHERE " % self.db_table_name

        if len(record) == len(self.keys):

            for index, element in enumerate(record):
                if index == 0:
                    sql_query_delete += "%s=?" % self.keys[index]
                else:
                    sql_query_delete += " and %s=?" % self.keys[index]
            sql_query_delete += ";"

            self.execute(sql_query_delete, record, commit=True)

        else:
            rm = "Not enough values to delete record in database"
            rc = ERROR_SQL_QUERY_VALUES

        return rc, rm


    def read_all(self):

        sql_query_select = "SELECT * FROM %s;" % self.db_table_name
        rc, rm, data = self.execute(sql_query_select, commit=False)
        return rc, rm, data


    def read_record(self, record):

        rc = 0; rm =""
        data = ()
        sql_query_select = "SELECT * FROM %s WHERE " % self.db_table_name

        if len(record) == len(self.keys):

            for index, element in enumerate(record):
                if index == 0:
                    sql_query_select += "%s = ?" % self.keys[index]
                else:
                    sql_query_select += " and %s = ?" % self.keys[index]

            sql_query_select += ";"
            rc, rm, data = self.execute(sql_query_select, record)

            if rc != 0:
                rm = "Error - cannot read from database [%s]." % self.db_name
        else:
            rm = "Wrong amount of values to update record from database"
            rc = ERROR_SQL_QUERY_VALUES

        return rc, rm, data


    def update_record_by(self, new_record):

        rc = 0; rm = ""; data = ()

        sql_query_update = "UPDATE %s SET " % self.db_table_name
        pk_indexes = {}
        pk_val = []; list_pk_name = []

        val_condition = {}
        if self.column != len(new_record):
            rm = "Wrong amount of values to update record from database"
            rc = ERROR_SQL_QUERY_VALUES
            return rc , rm
        else:
            # enumerate over table column name to create an update query
            for index, item in enumerate(self.items):
                if index == 0:
                    sql_query_update += "%s = ?" % item
                else:
                    sql_query_update += ", %s = ?" % item

                # check if column name is primary key, then get appropriate value from record used for the update
                if item in self.keys:
                    val_condition[item] = new_record[index]
                else:
                    continue

        # convert tuple to list
        val_new_record = list(new_record)
        sql_condition = " WHERE "

        for index, key in enumerate(val_condition.keys()):
            if index == 0:
                sql_condition += "%s = ?" % key
            else:
                sql_condition += " AND %s = ?" % key
            pk_val.append(val_condition[key])

        val_new_record = val_new_record + pk_val
        sql_condition += ";"
        sql_query_update += sql_condition

        rc, rm, data = self.execute(sql_query_update, val_new_record, commit=True)
        return rc, rm


if __name__ == "__main__":
    pass
