import os
import sqlite3
from sqlite3 import Error
from typing import Union, Tuple


class IDataTable:
    """
    Data table interface for sqlite library
    """

    def __init__(self, db_name: Union[str, None] = None) -> None:
        """
        db_name and table statement needs to be inputted
        """
        db_root_path: str = os.getcwd()
        # debug if testing here
        # db_root_path: str = 'db'
        self.table_statement: Union[str, None] = None
        self.db_path: Union[str, None] = os.path.join(db_root_path, 'sql_related', 'db', db_name)
        self.connection_obj = None
        self.table_name: Union[str, None] = None
        self.db_type = 'sqlite'

    def create_connection(self) -> None:
        """
        Author: Zarin
        create a database connection to a SQLite database
        function taken from sqliite tutorial site
        If no database exist, it will create a new one based on the name given
        """

        try:
            self.connection_obj = sqlite3.connect(self.db_path)
            # print(sqlite3.version)
            print("Connection created with database")
            self.initialize_settings()
        except Error as e:
            raise e

    def close_connection(self) -> None:
        """
         Author: Zarin
         Close connection established
         """

        if self.connection_obj:
            self.connection_obj.close()
        else:
            print("Connection doesn't exist")

    def initialize_settings(self):
        """
        Initialize settings of sqlite.
        Override this method if custom initial settings for a datatable is needed
        Returns:

        """
        if self.connection_obj:
            try:
                setting_script = """
                PRAGMA foreign_keys = ON; /*To allow foreign key constraint to be used*/
                """
                c = self.connection_obj.cursor()
                result = c.execute(setting_script)

            except Error as e:
                # print(e)
                raise e

    def create_table(self) -> None:
        """
        Author: Zarin
        creates new table based on table_statement
        """
        if self.connection_obj:
            try:
                c = self.connection_obj.cursor()
                c.execute(self.table_statement)
            except Error as e:
                raise e

    def check_table_exist(self) -> bool:
        """
        Author: Zarin
        check if table exist in database
        """
        if self.connection_obj:
            try:
                # for getting name without count
                # search_script = "SELECT name FROM sqlite_master WHERE type='table' AND name='"
                # + self.table_name + "';"
                search_script = "SELECT count(name) FROM sqlite_master WHERE type='table' AND name='" \
                                + self.table_name + "';"
                c = self.connection_obj.cursor()
                exist_flag = c.execute(search_script)
                flag = exist_flag.fetchone()
                if flag[0] == 1:
                    return True
                elif flag[0] == 0:
                    return False
                elif flag[0] > 1:
                    raise "More than one of said table exist in database"
            except Error as e:
                # print(e)
                raise e

    def check_row_exist(self, column_header_name, searched_val) -> bool:
        """
        Author: Zarin
        check if row exist in database
        """
        if self.connection_obj:
            try:

                query_script = "SELECT EXISTS (SELECT 1 FROM " \
                               + self.table_name + \
                               " WHERE " + str(column_header_name) + " = '" + str(searched_val) + "' );"

                c = self.connection_obj.cursor()
                exist_flag = c.execute(query_script)
                flag = exist_flag.fetchone()
                if flag[0] == 1:
                    return True
                elif flag[0] == 0:
                    return False
                elif flag[0] > 1:
                    raise "More than one of said row exist in database"
            except Error as e:
                # print(e)
                raise e

    def _insert_to_table(self, input_dict: dict) -> bool:
        """
        Author: Zarin

        - Takes dict and preprocess it to a format suitable to input to sqlite
        - Insert to sqlite
        Args:
            input_dict: dictionary of inputs to be inserted to corresponding table
        Returns:
            None
        """
        try:
            # Parse the input dict, prepare it to suitable format
            input_keys = "(" + (",".join([x for x in input_dict.keys()])) + ")"
            input_values_placeholder = "(" + (",".join(["?" for x in input_dict.values()])) + ")"
            input_values = tuple([x for x in input_dict.values()])

            # placed the into insert_script
            commit_script = "INSERT INTO " + self.table_name + str(input_keys) + \
                            " VALUES" + str(input_values_placeholder)

            c = self.connection_obj.cursor()
            # current insert_into_table method uses the old python pattern,
            # where you need pass a placeholder of question marks and pass through the values as well
            # the great thing about the old method is that, if None value is inserted in input values, sqlite's api will
            # automatically convert it to Null and you dont have to conver None to Null string in the 1st place
            result = c.execute(commit_script, input_values)
            # result = c.execute(commit_script)
            # Inserting data doesn't have any return value
            self.connection_obj.commit()
            return True
        except Error as e:
            raise e

    def _delete_from_table(self, primary_key: dict) -> bool:
        """
        Author: Zarin

        Delete row based on given primary key and its value passed

        Args:
            primary_key: key->primary id, value-> value of corresponding data row to be deleted

        Returns:
            None

        """
        try:
            if len(primary_key) > 1:
                raise ValueError("Input has more than one element. For deleting, only one element is accepted")

            # Parse the input dict, prepare it to suitable format
            input_keys: list = [x for x in primary_key.keys()]
            input_values_placeholder: str = "(" + (",".join(["?" for x in primary_key.values()])) + ")"
            input_values = [x for x in primary_key.values()]

            # placed the into insert_script
            # delete_script = 'DELETE FROM tasks WHERE id=?'
            commit_script = "DELETE FROM " + self.table_name + \
                            " WHERE " + input_keys[0] + "=" + "'" + input_values[0] + "'"

            c = self.connection_obj.cursor()
            result = c.execute(commit_script, primary_key)
            # result = c.execute(commit_script)
            # deleting doesn't have any return value
            self.connection_obj.commit()
            return True
        except (Error, ValueError) as e:
            # print(e)
            raise e

    def _alter_table(self, set_dict: dict, where_dict: dict) -> bool:
        """
        Author: Zarin

        Protected Method
        Alters the table based on passed argument
        However, primary key column needs to be passed first
        Just realized, WHERE can only have one conditionals lol

        Args:
            where_dict: for where script
            set_dict: for set script

        Returns:
            None

        """
        try:

            # Parse the input dict, prepare it to suitable format
            # We'll use the placeholder method, cause it makes it easier to pass Null

            # input_keys = "= ?,".join([x for x in input_dict.keys()])
            # input_values_placeholder = "(" + (",".join(["?" for x in input_dict.values()])) + ")"
            # input_values = tuple([x for x in input_dict.values()])
            set_key = [x for x in set_dict.keys()]
            set_value = [x for x in set_dict.values()]
            len_set = len(set_dict)

            where_key = [x for x in where_dict.keys()]
            where_value = [x for x in where_dict.values()]
            len_where = len(where_dict)

            input_values_placeholder: str = "(" + (",".join(["?" for x in range(len_set + len_where)])) + ")"
            input_values = []

            # Set script maker
            set_script = " SET "
            loop_range_set = range(len_set)
            for curr_key, curr_value, index in zip(set_key, set_value, loop_range_set):
                # set_script += str(curr_key) + " = " + "'" + str(curr_value) + "'"
                set_script += str(curr_key) + " = " + "?"
                input_values.append(curr_value)
                # add comma if not last
                if index != loop_range_set[-1]:
                    set_script += ", "

            # Where script maker
            where_script = " WHERE "
            loop_range_where = range(len_where)
            for curr_key, curr_value, index in zip(where_key, where_value, loop_range_where):
                # where_script += str(curr_key) + " = " + "'" + str(curr_value) + "'"
                where_script += str(curr_key) + " = " + "?"
                input_values.append(curr_value)
                # add comma if not last
                if index != loop_range_where[-1]:
                    where_script += ", "

            # placed the into insert_script
            # sql = ''' UPDATE tasks
            #           SET priority = ? ,
            #               begin_date = ? ,
            #               end_date = ?
            #           WHERE id = ?'''

            # commit_script = "UPDATE " + self.table_name + \
            #                 " SET " + str(input_key[0]) + " = " + "'" + str(input_value[0]) + "'" + \
            #                 " WHERE " + str(input_primary_key[0]) + " = " + "'" + str(input_primary_value[0]) + "' "

            commit_script = "UPDATE " + self.table_name + set_script + where_script

            c = self.connection_obj.cursor()
            result = c.execute(commit_script, input_values)
            # result = c.execute(commit_script)
            self.connection_obj.commit()
            return True
        except (Error, ValueError) as e:
            # print(e)
            raise e

    def _get_all_data_from_table(self, table_name) -> Tuple[list, list]:
        """
        Author: Zarin

        Protected Method
        Query all rows in requested table
        Also returns column_names
        Args:
            table_name: table to get data from

        Returns:
            rows: rows of data of the table
            row_names: names of the columns

        """
        try:
            commit_script = "SELECT * FROM " + table_name

            c = self.connection_obj.cursor()
            # c.row_factory = sqlite3.Row
            c.execute(commit_script)
            rows = c.fetchall()
            row_names = [description[0] for description in c.description]  # get column names and return it as well
            return rows, row_names
        except (Error, ValueError) as e:
            # print(e)
            raise e

    def get_all_data(self) -> Tuple[list, list]:
        """
        Author: Zarin

        Get all data from table
        Returns:

        """
        return self._get_all_data_from_table(self.table_name)

    def _get_specific_data_from_table(self, get_dict: dict, requested_data="all"):
        """
        Author: Zarin

        Protected Method
        Get specific data from a datatable

        Args:
            get_dict:
             key tells the column label to search the value to get,
             value tells the value that the row contains that needs to be searched

            requested_data:
            　data requested to return when the row with the above condition is found
              if set 'all', all of the row's data is returned

        Returns:
            May return more than one row if the conditions meet

        """
        try:
            if len(get_dict) > 1:
                raise ValueError("Input has more than one element. For deleting, only one element is accepted")

            # Parse the input dict, prepare it to suitable format
            get_key: list = [x for x in get_dict.keys()]
            # input_values_placeholder: str = "(" + (",".join(["?" for x in primary_key.values()])) + ")"
            get_val = [x for x in get_dict.values()]

            if requested_data == "all":
                requested_data = " * "

            commit_script = "SELECT " + requested_data + \
                            " FROM " + self.table_name + \
                            " WHERE " + str(get_key[0]) + "=" + "'" + str(get_val[0]) + "'"

            c = self.connection_obj.cursor()
            # result = c.execute(commit_script, get_dict)
            result = c.execute(commit_script)
            # deleting doesn't have any return value
            self.connection_obj.commit()
            return result.fetchall()
        except (Error, ValueError) as e:
            # print(e)
            raise e

    def _get_specific_col_from_table(self, col_name: list):
        """
        Author: Zarin

        Protected Method
        Get specific column from table

        Args:
            col_name: names of col from dt to get. Can be multiple
        Returns:
            May return more than one row if the conditions meet

        """

        try:
            # Parse the input dict, prepare it to suitable format

            col_string = ""

            for each_col_name, index in zip(col_name, range(len(col_name))):
                col_string += str(each_col_name)
                if index != range(len(col_name))[-1]:
                    col_string += ", "

            commit_script = "SELECT " + col_string + \
                            " FROM " + self.table_name

            c = self.connection_obj.cursor()
            # result = c.execute(commit_script, get_dict)
            result = c.execute(commit_script)
            # deleting doesn't have any return value
            self.connection_obj.commit()
            return result.fetchall()
        except (Error, ValueError) as e:
            # print(e)
            raise e

    def refresh(self) -> None:
        """
        Refreshes database by just using conn.commit()
        Returns:

        """

        try:
            c = self.connection_obj.cursor()
            self.connection_obj.commit()
        except Error as e:
            # print(e)
            raise e

    def initialize_table(self) -> None:
        """
        Author: Zarin
         - Initialize connection
         - Check if requested table exist, otherwise create the table
        """
        self.create_connection()
        self.table_exist = self.check_table_exist()
        if not self.table_exist:
            self.create_table()
            self.table_exist = True

    def modify_existing_value(self, set_dict: dict, where_dict: dict):
        """
        Modify any value in the datatable
        Args:

            set_dict:
            new values to be placed to the pinpointed row
            can be multiple

            where_dict:
            Info of the rows to target.
            Key is the name of column
            Value is the current value to be targeted.
            ONLY ONE VALUE CAN BE ACCEPTED


        Returns:

        """

        # input_dict = tar
        if len(where_dict) > 1:
            raise ValueError("More than 1 target value received.")

        op_flag = self._alter_table(set_dict, where_dict)

        return op_flag

    def modify_primary_key(self, target_prim_key, new_prim_key):
        """
        Virtual Method
        To modify primary key of the corresponding datatable
        Args:
            target_prim_key:
            new_prim_key:

        Returns:

        """
        pass

    def get_specific_data_row(self, search_key: str, search_value, return_range='all'):
        """

        Args:
            search_key:
            search_value:
            return_range:
             specify what range of data from the row to be returned. If "all", then the whole row will be returned

        Returns:

        """
        search_dict: dict = {search_key: search_value}

        op_flag = self._get_specific_data_from_table(search_dict, return_range)

        return op_flag

    def get_specific_data_col(self, col_name: list):
        """
        Method to get specific data col. Can get multiple
        Args:
            col_name: List of col names needed

        Returns:

        """

        op_flag = self._get_specific_col_from_table(col_name)

        return op_flag

    def insert_data(self, **kwargs):
        """
        Virtual method
        Insert data as a full set to datatable
        Args:
            *args:

        Returns:

        """
        pass

    def delete_row(self, primary_key_val):
        """
        Virtual method
        Delete row with corresponding primary key value
        The primary key name needs to be hardcoded for each datatable class
        Args:
            room_num:

        Returns:

        """
        # primary_key = 'example'
        #
        # input_dict = {primary_key: primary_key_val}
        #
        # op_flag = self._delete_from_table(input_dict)
        #
        # return op_flag
        pass
