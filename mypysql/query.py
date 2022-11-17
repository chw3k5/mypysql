import time
from copy import deepcopy
from collections import deque
from operator import attrgetter
from typing import NamedTuple, Union, Optional
from mypysql.sql import OutputSQL


def is_num(test_str):
    try:
        float(test_str)
        return True
    except ValueError:
        return False


def num_format(a_string):
    if isinstance(a_string, (float, int)):
        return a_string
    elif a_string is None:
        return None
    try:
        return int(a_string)
    except ValueError:
        try:
            return float(a_string)
        except ValueError:
            return a_string


class XYQuery(NamedTuple):
    query_type: str
    x_type: str
    y_type: str
    conditions: list


class TableQuery(NamedTuple):
    query_type: str
    data_types: list
    conditions: list


class Condition(NamedTuple):
    logic_prefix: str
    target_type: str
    comparator: str
    target_value: str
    table_name: Optional[str] = None
    start_parentheses: Optional[int] = 0
    end_parentheses: Optional[int] = 0

    def __str__(self):
        return_str = F"{self.logic_prefix} "
        for counter in range(self.start_parentheses):
            return_str += "("
        if self.table_name is not None:
            return_str += F"{self.table_name}.{self.target_type} {self.comparator} "
        else:
            return_str += F"{self.target_type} {self.comparator} "
        if is_num(self.target_value):
            return_str += F"{self.target_value}"
        else:
            return_str += F"'{self.target_value}'"
        for counter in range(self.end_parentheses):
            return_str += ")"
        return return_str


class SingleParam(NamedTuple):
    value: Union[float, int, str]
    param: Optional[str] = None
    err: Optional[Union[float, int, str, tuple]] = None
    ref: Optional[str] = None
    units: Optional[str] = None
    notes: Optional[str] = None


def parse_conditions(raw_conditions):
    pro_conditions = []
    for raw_condition in raw_conditions:
        logic_prefix, start_parentheses_test, target_type, comparator, target_value, end_parentheses_test \
            = raw_condition.split("|")
        pro_conditions.append(Condition(logic_prefix=logic_prefix.strip().upper(), target_type=target_type.strip(),
                                        comparator=comparator.strip(), target_value=target_value.strip(),
                                        start_parentheses=start_parentheses_test.count("("),
                                        end_parentheses=end_parentheses_test.count(")")))
    return pro_conditions


def parse_query_str(query_str):
    # This is the most base rule of the query parser
    # Only query_type and num_of_params need to have values, query_details is a list that is allowed to be empty
    query_type, num_of_param, *query_details = query_str.split(",")
    # expected to be all lowercase letter
    query_type = query_type.lower()
    # expected to be an int
    num_of_param = int(num_of_param)
    # Error here is the query_type is not expected
    if query_type in {"plot", "table", "plot!", "table!"}:
        # Additional qualifications can be added here to broaden the types of queries handled.
        if len(query_details) < num_of_param:
            data_types, conditions = query_details, []
        else:
            data_types, conditions = query_details[:num_of_param], query_details[num_of_param:]
        return TableQuery(query_type=query_type, data_types=data_types, conditions=parse_conditions(conditions))
    else:
        raise KeyError(F"Query type {query_type} is not valid.")


links_to_data = {"_param", "_value", "_errorlow", "_errorhigh", "_ref", "_units"}


def sql_columns_str(table_type, parameter_list, table_param_alias, table_str, output_header,
                    join_clauses, where_clauses, counter, prime_key, join_type="INNER"):
    """
    An abstracted MySQL String Query. This is a fundamental tool in to join parameters tables in the spexodisks database
    This abstraction handles the messy parts joining an arbitrary number of parameter types together, while maintaining
    a standard output format.

    Many of the input and output variables are the same, as this function is meant to run inside a loop where these
    variables are iteratively updated.

    :param table_type:
    :param parameter_list:
    :param table_param_alias:
    :param table_str:
    :param output_header:
    :param join_clauses:
    :param where_clauses:
    :param counter:
    :param prime_key:
    :param join_type:
    :return:
    """
    for table_name, data_type in parameter_list:
        if table_type == "spectra":
            prime_key = 'spectrum_handle'
        output_header += F"{data_type}_param,{data_type}_value,{data_type}_errorlow,{data_type}_errorhigh,"
        output_header += F"{data_type}_ref,{data_type}_units,"
        alias = F"param_type_{counter}"
        table_param_alias[(data_type, table_name)] = alias
        if table_type == "object_params_float":
            table_str += F'''{alias}.float_param_type AS 'param_{counter}', '''
            table_str += F'''{alias}.float_value AS 'value_{counter}', '''
            table_str += F'''{alias}.float_error_low AS 'error_low_{counter}', '''
            table_str += F'''{alias}.float_error_high AS 'error_high_{counter}', '''
            table_str += F'''{alias}.float_ref AS 'ref_{counter}', '''
            table_str += F'''{alias}.float_units AS 'units_{counter}', '''
            join_clauses.append(F'''{join_type} JOIN {table_name} AS `{alias}` ON ''' +
                                F'''h.spexodisks_handle = {alias}.spexodisks_handle ''')
            where_clauses.append(F'''{alias}.float_param_type = "{data_type}"''')
        if table_type == "object_params_str":
            table_str += F'''{alias}.str_param_type AS 'param_{counter}', '''
            table_str += F'''{alias}.str_value AS 'value_{counter}', '''
            table_str += F'''{alias}.str_error AS 'error_low_{counter}', '''
            table_str += F'''{alias}.str_error AS 'error_high_{counter}', '''
            table_str += F'''{alias}.str_ref AS 'ref_{counter}', '''
            table_str += F'''{alias}.str_units AS 'units_{counter}', '''
            join_clauses.append(F'''{join_type} JOIN {table_name} AS `{alias}` ON ''' +
                                F'''h.spexodisks_handle = {alias}.spexodisks_handle ''')
            where_clauses.append(F'''{alias}.str_param_type = "{data_type}"''')
        if table_type == "spectra":
            table_str += F'''"{data_type}" AS 'param_{counter}', '''
            table_str += F'''{alias}.{data_type} AS 'value_{counter}', '''
            table_str += F'''"NULL" AS 'error_low_{counter}', '''
            table_str += F'''"NULL" AS 'error_high_{counter}', '''
            table_str += F'''{alias}.spectrum_reference AS 'ref_{counter}', '''
            table_str += F'''"NULL"  AS 'units_{counter}', '''
            join_clauses.append(F'''{join_type} JOIN {table_name} AS `{alias}` ON ''' +
                                F'''h.spectrum_handle = {alias}.spectrum_handle ''')
            prime_key = 'spectrum_handle'
        counter += 1
    return table_param_alias, table_str, output_header, join_clauses, where_clauses, counter, prime_key


def format_output(unformatted_output,
                  header="spexodisks_handle,param_x,value_x,error_low_x,error_high_x,ref_x," +
                         "units_x,param_y,value_y,error_low_y,error_high_y,ref_y,units_y",
                  prime_key="spexodisks_handle"):
    """
    The output MySQL data has a lot of repeated values as a result of outer joins.

    We are going to collapse that output into a more user friendly container that is the shape that we want.

    To keep things simple, all the outputs are sorted lists. However, we first leverage some of python's other
    faster variable types to reshape the data and get rid of some duplicates.
    """
    # The header is
    header = header.split(',')
    # Data columns have a mapping, name_columns have no extra mapping, here we determine the mapping and keep the order
    handle_dict = {}
    name_columns = []
    data_columns = []
    data_columns_set = set()
    data_column_map = {}
    for column_name in header:
        name_column_type = True
        for data_link in links_to_data:
            if data_link in column_name:
                name_column_type = False
                prime_data_type = column_name.replace(data_link, "")
                data_column_map[column_name] = (prime_data_type, data_link.replace("_", ""))
                if prime_data_type not in data_columns_set:
                    data_columns_set.add(prime_data_type)
                    data_columns.append(prime_data_type)
        if name_column_type:
            name_columns.append(column_name)
    # we now know the shape of the output, we will initial that form here
    formatted_columns = name_columns + data_columns
    blank_output = tuple([set() for _ in range(len(name_columns) + len(data_columns))])

    # Now we start on the raw sql data
    for output_row in unformatted_output:
        # initialize and do a little formatting of the raw data values
        row_dict = {key.strip().lower(): num_format(value) for key, value in zip(header, output_row)}
        prime_key_this_row = row_dict[prime_key]
        if prime_key_this_row not in handle_dict.keys():
            handle_dict[prime_key_this_row] = deepcopy(blank_output)
        # fold the data in the right shape for the output
        folded_row_dict = {}
        for column_name in header:
            if column_name in data_column_map.keys():
                prime_data_type, data_link = data_column_map[column_name]
                if prime_data_type not in folded_row_dict.keys():
                    folded_row_dict[prime_data_type] = {}
                folded_row_dict[prime_data_type][data_link] = row_dict[column_name]
            else:
                folded_row_dict[column_name] = row_dict[column_name]
        # now the folded data is into a special tuple
        for column_index, output_column in list(enumerate(formatted_columns)):
            if output_column in data_columns_set:
                datum_dict = folded_row_dict[output_column]
                formatted_datum = SingleParam(value=datum_dict["value"], param=datum_dict["param"],
                                              err=(datum_dict["errorlow"], datum_dict["errorhigh"]),
                                              ref=datum_dict["ref"], units=datum_dict["units"])
            else:
                formatted_datum = folded_row_dict[output_column]
            # this is the final fold of the data, by adding it to a set we only save unique data
            handle_dict[prime_key_this_row][column_index].add(formatted_datum)
    # While not required, here we turn things into ordered lists. This is good for delivering uniform results
    output = []
    for prime_key in sorted(handle_dict.keys()):
        temp_data_holder = [prime_key]
        for column_index, output_column in list(enumerate(formatted_columns)):
            if output_column in data_columns_set:
                temp_data_holder.append(sorted(handle_dict[prime_key][column_index], key=attrgetter("value")))
            else:
                temp_data_holder.append(sorted(handle_dict[prime_key][column_index]))
        output.append(tuple(temp_data_holder))
    return output


class QueryEngine:
    query_log = deque(maxlen=int(1e4))

    def __init__(self, database='spexodisks'):
        self.output_sql = OutputSQL()
        self.params_str = {item[0] for item in
                           self.output_sql.query(
                               sql_query_str=F"SELECT str_params FROM {database}.available_str_params")}

        self.params_float = {item[0] for item in
                             self.output_sql.query(
                                 sql_query_str=F"SELECT float_params FROM {database}.available_float_params")}
        self.object_float_params_fields = {item[0] for item in
                                           self.output_sql.query(
                                               F"""SELECT COLUMN_NAME AS spectrum_params
                                                FROM INFORMATION_SCHEMA.COLUMNS 
                                                WHERE TABLE_NAME = 'object_params_float';""")}

        self.params_spectrum = {item[0] for item in
                                self.output_sql.query(
                                    sql_query_str=F"SELECT spectrum_params FROM {database}.available_spectrum_params")}
        self.params_spectrum_str = {"spectrum_handle", "spexodisks_handle", "set_type", "pi", "reference",
                                    "data_reduction_by", "aor_key", "ref_frame", "output_filename"}
        self.params_spectrum_float = self.params_spectrum - self.params_spectrum_str
        self.object_params = self.params_str | self.params_float

    def close(self):
        self.output_sql.close()

    def base_query(self, parsed_query, join_type="LEFT OUTER"):
        # find out what tables the conditions reference
        table_added_conditions = {}
        for condition in parsed_query.conditions:
            target_table = self.data_type_to_table_location(condition.target_type)
            if target_table not in table_added_conditions.keys():
                table_added_conditions[target_table] = []
            table_added_conditions[target_table].append(Condition(logic_prefix=condition.logic_prefix,
                                                                  target_type=condition.target_type,
                                                                  comparator=condition.comparator,
                                                                  target_value=condition.target_value,
                                                                  table_name=F"spexodisks.{target_table}",
                                                                  start_parentheses=condition.start_parentheses,
                                                                  end_parentheses=condition.end_parentheses))
        # initialize
        prime_key = 'spexodisks_handle'
        table_param_alias = {}
        join_clauses = []
        where_clauses = []
        counter = 1
        table_str = 'SELECT h.spectrum_handle, ' + \
                    'h.spexodisks_handle,  ' + \
                    'h.pop_name, ' + \
                    'h.preferred_simbad_name, '
        output_header = 'spectrum_handle,spexodisks_handle,pop_name,preferred_simbad_name,'
        # make the strings for the SQL query
        for outer_join_data_type in parsed_query.data_types:
            table_location = self.data_type_to_table_location(outer_join_data_type)
            parameter_list = [(F'spexodisks.{table_location}', outer_join_data_type)]
            table_param_alias, table_str, output_header, join_clauses, where_clauses, counter, prime_key = \
                sql_columns_str(table_location, parameter_list, table_param_alias, table_str, output_header,
                                join_clauses, where_clauses, counter, prime_key, join_type=join_type)
        # Clean up and add join clauses to the table str
        output_header = output_header[:-1]
        table_str = table_str[:-2] + F''' FROM spexodisks.handles AS `h` '''
        for join_clause in join_clauses:
            table_str += str(join_clause)
        return table_str, output_header, table_added_conditions, where_clauses, counter, prime_key

    def data_type_to_table_location(self, data_type):
        if data_type in self.params_str or "str" in data_type:
            return "object_params_str"
        elif data_type in self.params_float or "float" in data_type:
            return "object_params_float"
        elif data_type in self.params_spectrum or "spectrum" in data_type:
            return "spectra"
        else:
            raise KeyError(F"Data type {data_type} is not valid.")

    def query_single(self, parsed_query, join_type="LEFT OUTER"):
        """
        Query string formatting:
        "return_data_format, num_of_data_types, x_data_type [, y_data_type, z_data_type, ...] [, conditions]"
        where square brackets, [], show optional strings.

        Whitespace is ignored, but are useful for clarity and making things easy to look at in test strings.
            x_data_type, y_data_type, ...: The supported data types are listed in a few simple tables on the MySQL database.
                                           These tables are generated as a part of the construction of the database,
                                           from Caleb Python pipeline. to look at these types ofr the current database:
                                               import QueryEngine # it is in this file
                                               qe = QueryEngine()
                                               # then see the allowed values in
                                               qe.params_str
                                               qe.params_float
                                               ge.object_float_params_fields
                                               qe.params_spectrum
                                               ge.params_spectrum_str
                                               ge.params_spectrum_float


        When "plot" == return_data_format the query string is formatted as
                "plot,num_of_data_types,x_data_type,[y_data_type, ...][,conditions]"

        When "table" == return_data_format the query string is formatted in the exact same way!
                "table,num_of_data_types,x_data_type,[y_data_type, ...][,conditions]"

        The difference is that 'plot' will use faster INNER JOINs that require all the returned data to have a value.
        For example, if 'plot' is selected, with parameters x_param and y_param but a given star only has values for
        x_param and not y_param then that data is not returned. This dat would not be good for a plot that needs one
        at least on x_param per y_param.

        Selecting 'table' means that an LEFT OUTER JOIN will be used. This process would return data in previous example,
        where a certain object is missing data for some parameters. This is useful fo table type data, where null values
        for some objects is fine.

        num_of_data_types: a number that tells you how many data types for the parser to expect.

        Conditions have a complex structure to maximize the potential of MYSQL.
        While initial parsing of the query string splits based on the comma ",",
        the conditions are split again be a secondary delimiter the pipe "|".
        Multiple conditions are split with a comma ",".
        The conditions format is:
            "logical_prefix|start_parentheses|target_type|comparator|target_value|end_parentheses"

        This mimics the MySQL structure. There is plenty of room to send in bad conditions or leave unclosed parentheses.
        Where possible, I have built in exceptions to stop bad queries.
            logical_prefix: This should be either {"and", "or"}. The parser is expecting the first condition to have "AND",
                            but that first logical_operator that is omitted final query.
            start_parentheses: This counts the number of "(" in the parsed string, can be 0 to n_int.
            target_type: same options as for x_data_type and y_data_type above.
            comparator: this can be any mySQL operator, the only thing the parser does is strip whitespace from the ends
                        of this string.
            target_value: this is a value is being compared. Values that are str, int, floats should all be entered as
                          strings. The parser will automatically add the '' required for string comparison for the
                          mySQL query.
            end_parentheses: This counts the number of ")" in the parsed string, can be 0 to n_int.



        Formatted output: a list with each element is a tuple with the format
            (key:str, spectrum_handles:list, spexodisks_handles, pop_names, preferred_simbad_name, x_params, y_params, ...).


        The output list is alphabetically sorted by the 'key', which is dynamically choose to be either the
        spexodisks_handle or the spectrum_handle when spectrum data is return.
        x_data_list and y_data_list, ect., will be list with a
        length of at least 1, but more are possible many more depending on how much data was found.
        The elements of x_data_list and y_data_list are NamedTuples and can be accessed using the "." or attribute
        structure.
        """
        start_time = time.perf_counter()
        table_str, output_header, table_added_conditions, where_clauses, counter, prime_key = \
            self.base_query(parsed_query=parsed_query, join_type=join_type)
        # Make inner joins and where statements
        conditions_where_clauses = []
        for main_table_type in sorted(table_added_conditions.keys()):
            alias = F"param_type_{counter}"
            table_str += F'''INNER JOIN spexodisks.{main_table_type} AS `{alias}` ON '''
            if main_table_type == "spectra":
                table_str += F'''h.spectrum_handle = {alias}.spectrum_handle '''
            else:
                table_str += F'''h.spexodisks_handle = {alias}.spexodisks_handle '''
            counter += 1
            for condition in table_added_conditions[main_table_type]:
                conditions_where_clauses.append(Condition(logic_prefix=condition.logic_prefix,
                                                          target_type=condition.target_type,
                                                          comparator=condition.comparator,
                                                          target_value=condition.target_value,
                                                          table_name=F"{alias}",
                                                          start_parentheses=condition.start_parentheses,
                                                          end_parentheses=condition.end_parentheses))
        if any([where_clauses != [], conditions_where_clauses != []]):
            table_str += F'''WHERE '''
            is_first_condition = True
            # required to join the tables
            if where_clauses:
                table_str += F'''('''
            for where_clause in where_clauses:
                is_first_condition = False
                table_str += F'''{where_clause} AND '''
            if where_clauses:
                table_str = table_str[:-5] + F''') '''
            # conditions that act on returned data
            if conditions_where_clauses:
                for condition in conditions_where_clauses:
                    single_condition = F"""{condition} """
                    if is_first_condition:
                        table_str += single_condition.replace("AND ", "")
                        is_first_condition = False
                    else:
                        table_str += single_condition

        table_str += ";"
        raw_sql_output = self.output_sql.query(sql_query_str=table_str)
        formatted_output = format_output(unformatted_output=raw_sql_output, header=output_header, prime_key=prime_key)
        end_time = time.perf_counter()
        self.query_log.appendleft(F"{join_type}|{end_time - start_time}|{table_str}")
        return formatted_output

    def query_double(self, parsed_query, join_type="INNER"):
        start_time = time.perf_counter()
        table_str, output_header, table_added_conditions, where_clauses, counter, prime_key = \
            self.base_query(parsed_query=parsed_query, join_type=join_type)
        if where_clauses:
            table_str += F'''WHERE '''
            table_str += F'''('''
        for where_clause in where_clauses:
            table_str += F'''{where_clause} AND '''
        if where_clauses:
            table_str = table_str[:-5] + F''') '''
        table_name = join_type.replace(" ", "")
        for name in sorted(parsed_query.data_types):
            table_name += name

        user_table_name = self.output_sql.user_table(table_str=table_str, user_table_name=table_name)

        conditions_query_str = F"""SELECT DISTINCT * """
        conditions_query_str += F"""FROM temp.{user_table_name} """

        for table_name in table_added_conditions.keys():
            conditions_query_str += F"""INNER JOIN spexodisks.{table_name} """
            conditions_query_str += F"""ON temp.{user_table_name}.spexodisks_handle = """
            conditions_query_str += F"""spexodisks.{table_name}.spexodisks_handle """

        if table_added_conditions:
            conditions_query_str += F"""WHERE """
        is_first_condition = True
        for table_name in table_added_conditions.keys():
            for condition in table_added_conditions[table_name]:
                single_condition = F"""{condition} """
                if is_first_condition:
                    conditions_query_str += single_condition.replace("AND ", "")
                    is_first_condition = False
                else:
                    conditions_query_str += single_condition
        conditions_query_str += ";"
        raw_sql_output = self.output_sql.query(sql_query_str=conditions_query_str)
        formatted_sql_output = format_output(unformatted_output=raw_sql_output, header=output_header,
                                             prime_key=prime_key)
        end_time = time.perf_counter()
        self.query_log.appendleft(F"query_double|{join_type}|{end_time - start_time}|{conditions_query_str}")
        return formatted_sql_output

    def query(self, query_str):
        # parse the query, this state is what each method will be expecting.
        parsed_query = parse_query_str(query_str=query_str)
        parsed_query_type = parsed_query.query_type
        # detect a modifier that selects between a single query (all data in one query)
        # and double query (all data after two queries to the MySQL Server)
        if parsed_query_type[-1] == "!":
            query_method = self.query_double
            parsed_query_type = parsed_query_type[:-1]
        else:
            query_method = self.query_single

        # Select between SQL JOIN types
        if parsed_query_type == "plot":
            join_type = "INNER"
        elif parsed_query_type == "table":
            join_type = "LEFT OUTER"
        else:
            raise KeyError(F"Query type {parsed_query_type} is not valid.")
        # The requested Query is now performed.
        return query_method(parsed_query=parsed_query, join_type=join_type)

    def curated_query(self, params=None, database='spexodisks'):
        """

        :param params: None or list of str. None returns all columns of data. As a list of strings,
                       this argument controls what columns of data are returned.
        :param database: str. Default is the main MySQL metadata database 'spexodisks'. However you may want to
                         query other test d
        :return: A pandas dataframe. The first three columns are object names
                 (spexodisks_handle, preferred_simbad_name, pop_name), this is also what an empty list returns.
                 The for each string in the 'params' list a four columns quad is returned in the pandas dataframe.
                 The quad order is the order of the 'params' list. The quad format is:
                 (param_value, param_err_high, param_err_low, param_ref).

        """
        if params is None:
            columns_str = '*'
        else:
            columns_str = '''"spexodisks_handle", "preferred_simbad_name", "pop_name"'''
            # test the parameters and to make sure the request can be fulfilled.
            for test_param in params:
                if test_param not in self.object_params:
                    raise KeyError(F"{test_param} was a requested parameters that is not listed in the " +
                                   F"available object parameters:\n {self.object_params}")
                columns_str += F''', "{test_param}_value", "{test_param}_err_high", "{test_param}_err_low", 
                                   "{test_param}_ref"'''
        # request the query
        self.output_sql.cursor.execute(F'''SELECT {columns_str} FROM {database}.curated''')
        # get the returned data
        table_rows = self.output_sql.cursor.fetchall()
        return table_rows

