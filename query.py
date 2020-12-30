from typing import NamedTuple, Union, Optional
from mypysql.sql import OutputSQL
from operator import attrgetter,itemgetter
import numpy as np
import matplotlib.pyplot as plt


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


def local_polar_coordinates(x, y, x_offset, y_offset):
    x_local = x - x_offset
    y_local = y - y_offset
    r = np.sqrt((x_local**2.0) + (y_local**2.0))
    phi = np.arctan2(x_local, y_local)
    return r, phi


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
    query_type, *query_details = query_str.split(",")
    try:
        num_of_plot_columns = int(query_type)
        query_type = "table"
    except ValueError:
        num_of_plot_columns = None
    if query_type == "table" and num_of_plot_columns is not None:
        data_types, conditions = query_details[:num_of_plot_columns], query_details[num_of_plot_columns:]
        return TableQuery(query_type=query_type, data_types=data_types, conditions=parse_conditions(conditions))
    elif query_type == "xy_plot":
        x_type, y_type, *conditions = query_details
        return XYQuery(query_type=query_type.strip(), x_type=x_type.strip(), y_type=y_type.strip(),
                       conditions=parse_conditions(conditions))
    else:
        raise KeyError(F"Query type {query_type} is not valid.")


def format_output(unformatted_output, header="spexodisks_handle,param_x,value_x,error_low_x,error_high_x,ref_x," +
                                             "units_x,param_y,value_y,error_low_y,error_high_y,ref_y,units_y"):
    handle_dict = {}
    for output_row in unformatted_output:
        row_dict = {key.strip().lower(): num_format(value) for key, value in zip(header.split(','), output_row)}
        spexodisks_handle = row_dict["spexodisks_handle"]
        if spexodisks_handle not in handle_dict.keys():
            handle_dict[spexodisks_handle] = (set(), set())
        x_params = SingleParam(value=row_dict["value_x"], param=row_dict["param_x"],
                               err=(row_dict["error_low_x"], row_dict["error_high_x"]),
                               ref=row_dict["ref_x"], units=row_dict["units_x"])
        handle_dict[spexodisks_handle][0].add(x_params)
        y_params = SingleParam(value=row_dict["value_y"], param=row_dict["param_y"],
                               err=(row_dict["error_low_y"], row_dict["error_high_y"]),
                               ref=row_dict["ref_y"], units=row_dict["units_y"])
        handle_dict[spexodisks_handle][1].add(y_params)
    output = []
    for key in sorted(handle_dict.keys()):
        x_set, y_set = handle_dict[key]
        output.append((key, sorted(x_set, key=attrgetter("value")), sorted(y_set, key=attrgetter("value"))))
    return output


class QueryEngine:
    def __init__(self):
        self.output_sql = OutputSQL()
        self.params_str = {item[0] for item in
                           self.output_sql.query(
                               sql_query_str="SELECT str_params FROM spexodisks.available_str_params")}

        self.params_float = {item[0] for item in
                             self.output_sql.query(
                                 sql_query_str="SELECT float_params FROM spexodisks.available_float_params")}
        self.object_float_params_fields = {item[0] for item in
                                           self.output_sql.query(
                                            F"""SELECT COLUMN_NAME AS spectrum_params
                                        FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'object_params_float';""")}

        self.params_spectrum = {item[0] for item in
                                self.output_sql.query(
                                    sql_query_str="SELECT spectrum_params FROM spexodisks.available_spectrum_params")}
        self.params_spectrum_str = {"spectrum_handle", "spexodisks_handle", "set_type", "pi", "reference",
                                    "data_reduction_by", "aor_key", "ref_frame", "output_filename"}
        self.params_spectrum_float = self.params_spectrum - self.params_spectrum_str

    def close(self):
        self.output_sql.close()

    def property_to_table(self, data_type):
        if data_type in self.params_str:
            return "object_params_str"
        elif data_type in self.params_float | self.object_float_params_fields:
            return "object_params_float"
        elif data_type in self.params_spectrum:
            return "spectra"
        else:
            raise KeyError

    def query_xy_plot(self, parsed_query):
        required_tables = set()
        # figure out what tables the x_type and y_type belong to.
        if parsed_query.x_type in self.params_float:
            x_table = "object_params_float"
        elif parsed_query.x_type in self.params_spectrum_float:
            x_table = "spectra"
        else:
            raise KeyError(F"Data type {parsed_query.x_type} is not valid for the query_xy_plot.")
        required_tables.add(x_table)
        if parsed_query.y_type in self.params_float:
            y_table = "object_params_float"
        elif parsed_query.y_type in self.params_spectrum_float:
            y_table = "spectra"
        else:
            raise KeyError(F"Data type {parsed_query.y_type} is not valid for the query_xy_plot.")
        required_tables.add(y_table)

        # find out what tables the conditions reference
        table_added_conditions = {}
        for condition in parsed_query.conditions:
            target_table = self.property_to_table(condition.target_type)
            if target_table not in table_added_conditions.keys():
                table_added_conditions[target_table] = []
            table_added_conditions[target_table].append(Condition(logic_prefix=condition.logic_prefix,
                                                                  target_type=condition.target_type,
                                                                  comparator=condition.comparator,
                                                                  target_value=condition.target_value,
                                                                  table_name=F"spexodisks.{target_table}",
                                                                  start_parentheses=condition.start_parentheses,
                                                                  end_parentheses=condition.end_parentheses))
            required_tables.add(target_table)
        # Join the tables the contain the x and y data sets
        if x_table == "object_params_float":
            param_x, value_x, error_low_x = "x_table.float_param_type", "x_table.float_value", "x_table.float_error_low"
            error_high_x, ref_x, units_x = "x_table.float_error_high", "x_table.float_ref", "x_table.float_units"
        elif x_table == "spectra":
            param_x, value_x, error_low_x = F"'{parsed_query.x_type}'", F"x_table.{parsed_query.x_type}", "NULL"
            error_high_x, ref_x, units_x = "NULL", "x_table.spectrum_reference", "NULL"
        else:
            raise KeyError
        if y_table == "object_params_float":
            param_y, value_y, error_low_y = "y_table.float_param_type", "y_table.float_value", "y_table.float_error_low"
            error_high_y, ref_y, units_y = "y_table.float_error_high", "y_table.float_ref", "y_table.float_units"
        elif y_table == "spectra":
            param_y, value_y, error_low_y = F"'{parsed_query.y_type}'", F"y_table.{parsed_query.y_type}", "NULL"
            error_high_y, ref_y, units_y = "NULL", "y_table.spectrum_reference", "NULL"
        else:
            raise KeyError

        table_str = F"""SELECT DISTINCT x_table.spexodisks_handle,
                               {param_x} AS 'param_x', 
                               {value_x} AS 'value_x', 
                               {error_low_x}  AS 'error_low_x', 
                               {error_high_x} AS 'error_high_x', 
                               {ref_x} AS 'ref_x', 
                               {units_x} AS 'units_x',
                               {param_y} AS 'param_y', 
                               {value_y} AS 'value_y', 
                               {error_low_y} AS 'error_low_y', 
                               {error_high_y} AS 'error_high_y',
                               {ref_y} AS 'ref_y', 
                               {units_y} AS units_y
                         FROM spexodisks.{x_table} AS x_table
                            INNER JOIN spexodisks.{y_table} AS y_table
                                ON x_table.spexodisks_handle = y_table.spexodisks_handle
                         WHERE ({param_x} = "{parsed_query.x_type}"
                            AND {param_y} = "{parsed_query.y_type}");"""
        user_table_name = self.output_sql.user_table(table_str=table_str)

        conditions_query_str = F"""SELECT DISTINCT temp.{user_table_name}.spexodisks_handle, param_x, value_x, error_low_x, error_high_x, """
        conditions_query_str += F"""ref_x, units_x, param_y, value_y, error_low_y, error_high_y, ref_y, units_y """
        conditions_query_str += F"""FROM temp.{user_table_name} """

        for table_name in table_added_conditions.keys():
            conditions_query_str += F"""INNER JOIN spexodisks.{table_name} """
            conditions_query_str += F"""ON temp.{user_table_name}.spexodisks_handle = """
            conditions_query_str += F"""spexodisks.{table_name}.spexodisks_handle """
        else:
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
        raw_sql_output = self.output_sql.query(sql_query_str=conditions_query_str + ";")
        formatted_sql_output = format_output(unformatted_output=raw_sql_output)
        return formatted_sql_output

    def data_type_to_table_location(self, data_type):
        if data_type in self.params_str or "str" in data_type:
            return "object_params_str"
        elif data_type in self.params_float or "float" in data_type:
            return "object_params_float"
        elif data_type in self.params_spectrum or "spectrum" in data_type:
            return "spectra"
        else:
            raise KeyError(F"Data type {data_type} is not valid.")

    def query_table(self, parsed_query):
        parameters_by_table = {"spectra": set(), "object_params_str": set(), "object_params_float": set()}
        for outer_join_data_type in parsed_query.data_types:

            table_location = self.data_type_to_table_location(outer_join_data_type)
            parameters_by_table[table_location].add((F"spexodisks.{table_location}", outer_join_data_type))

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
        table_param_alias = {}
        join_clauses = []
        where_clauses = []
        counter = 1
        table_str = 'SELECT h.spectrum_handle, ' +\
                    'h.spexodisks_handle,  ' +\
                    'h.pop_name, ' +\
                    'h.preferred_simbad_name, '
        ### Data that will be returned
        # tables that are joined for the required return data
        for table_name, data_type in sorted(parameters_by_table["object_params_float"]):
            alias = F"param_type_{counter}"
            table_param_alias[(data_type, table_name)] = alias
            table_str += F'''{alias}.float_param_type AS 'param_{counter}', '''
            table_str += F'''{alias}.float_value AS 'value_{counter}', '''
            table_str += F'''{alias}.float_error_low AS 'error_low_{counter}', '''
            table_str += F'''{alias}.float_error_high AS 'error_high_{counter}', '''
            table_str += F'''{alias}.float_ref AS 'ref_{counter}', '''
            table_str += F'''{alias}.float_units AS 'units_{counter}', '''
            join_clauses.append(F'''LEFT OUTER JOIN {table_name} AS `{alias}` ON ''' +
                                F'''h.spexodisks_handle = {alias}.spexodisks_handle ''')
            where_clauses.append(F'''{alias}.float_param_type = "{data_type}"''')
            counter += 1

        for table_name, data_type in sorted(parameters_by_table["object_params_str"]):
            alias = F"param_type_{counter}"
            table_param_alias[(data_type, table_name)] = alias
            table_str += F'''{alias}.str_param_type AS 'param_{counter}', '''
            table_str += F'''{alias}.str_value AS 'value_{counter}', '''
            table_str += F'''{alias}.str_error AS 'error_low_{counter}', '''
            table_str += F'''{alias}.str_error AS 'error_high_{counter}', '''
            table_str += F'''{alias}.str_ref AS 'ref_{counter}', '''
            table_str += F'''{alias}.str_units AS 'units_{counter}', '''
            join_clauses.append(F'''LEFT OUTER JOIN {table_name} AS `{alias}` ON ''' +
                                F'''h.spexodisks_handle = {alias}.spexodisks_handle ''')
            where_clauses.append(F'''{alias}.str_param_type = "{data_type}"''')
            counter += 1

        for table_name, data_type in sorted(parameters_by_table["spectra"]):
            alias = F"param_type_{counter}"
            table_param_alias[(data_type, table_name)] = alias
            table_str += F'''{alias}.{data_type} AS 'param_{counter}', '''
            table_str += F'''{alias}.{data_type} AS 'value_{counter}', '''
            table_str += F'''"NULL" AS 'error_low_{counter}', '''
            table_str += F'''"NULL" AS 'error_high_{counter}', '''
            table_str += F'''{alias}.spectrum_reference AS 'ref_{counter}', '''
            table_str += F'''"NULL"  AS 'units_{counter}', '''
            join_clauses.append(F'''LEFT OUTER JOIN {table_name} AS `{alias}` ON ''' +
                                F'''h.spectrum_handle = {alias}.spectrum_handle ''')
            counter += 1

        table_str = table_str[:-2] + F''' FROM spexodisks.handles AS `h`'''
        for join_clause in join_clauses:
            table_str += str(join_clause)

        # sort out which conditions do not need new table joins
        conditions_that_need_joins = []
        aliased_conditions = []
        for condition in table_added_conditions:
            test_key = (condition.target_type, condition.table_name)
            if test_key in table_param_alias.keys():
                alias = table_param_alias[test_key]
                aliased_conditions.append(Condition(logic_prefix=condition.logic_prefix,
                                                                      target_type=condition.target_type,
                                                                      comparator=condition.comparator,
                                                                      target_value=condition.target_value,
                                                                      table_name=alias,
                                                                      start_parentheses=condition.start_parentheses,
                                                                      end_parentheses=condition.end_parentheses))
            else:
                conditions_that_need_joins.append(condition)

       # Test Here      

        # tables that must be joined because of conditions

        if any([where_clauses != []]):
            table_str += F'''WHERE '''
            # required to join the tables
            if where_clauses:
                table_str += F'''('''
            for where_clause in where_clauses:
                table_str += F'''{where_clause} AND '''
            if where_clauses:
                table_str = table_str[:-5] + F''') '''
            # conditions that act on returned data

            # conditions that filter based on data that is not returned

        user_table_name = self.output_sql.user_table(table_str=table_str)

        conditions_query_str = F"""SELECT DISTINCT temp.{user_table_name}.spexodisks_handle, param_x, value_x, error_low_x, error_high_x, """
        conditions_query_str += F"""ref_x, units_x, param_y, value_y, error_low_y, error_high_y, ref_y, units_y """
        conditions_query_str += F"""FROM temp.{user_table_name} """

        for table_name in table_added_conditions.keys():
            conditions_query_str += F"""INNER JOIN spexodisks.{table_name} """
            conditions_query_str += F"""ON temp.{user_table_name}.spexodisks_handle = """
            conditions_query_str += F"""spexodisks.{table_name}.spexodisks_handle """
        else:
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
        raw_sql_output = self.output_sql.query(sql_query_str=conditions_query_str + ";")
        formatted_sql_output = format_output(unformatted_output=raw_sql_output)
        return formatted_sql_output

    def query(self, query_str):
        parsed_query = parse_query_str(query_str=query_str)
        if parsed_query.query_type == "xy_plot":
            return self.query_xy_plot(parsed_query=parsed_query)
        elif parsed_query.query_type == "table":
            return self.query_table(parsed_query=parsed_query)
        else:
            raise KeyError(F"Query type {parsed_query.query_type} is not valid.")


if __name__ == "__main__":
    """
    Query string formatting:
    "return_data_format, x_data_type [, y_data_type] [, conditions]" where square brackets, [], show optional strings.
    Whitespace is ignored, but useful for clarity easy of look-up in the test strings.
        x_data_type, y_data_type: The supported data types are listed in a few simple tables on the MySQL database. 
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
                                        
    
    When "xy_plot" == return_data_format (the only working example on 12-20-2020) the query string is formatted as
                "xy_plot,x_data_type,y_data_type,[conditions]"
                
    Conditions have a complex structure to maximize of MYSQL. While initial parsing of the query string splits based on
    the comma ",", the conditions are split again be a secondary delimiter the pipe "|". Multiple conditions are split 
    with a comma ",". The conditions format is
        "logical_prefix|start_parentheses|target_type|comparator|target_value|end_parentheses"
    
    This mimics the MySQL structure. There is plenty of room to send in bad conditions, or leave unclose parentheses.
    Where possible, I have built in exceptions to stop bad queries.
        logical_prefix: This should be either {"and", "or"}. I am expecting the first condition list to have a "and"
                        logical_operator that is omitted for the SQL query.
        start_parentheses: This counts the number of "(" in the parsed string, can be 0 to n_int.
        target_type: same options as for x_data_type and y_data_type above.
        comparator: this can be any mySQL operator, the only thing the parser does is strip whitespace from the ends
                    of this string.
        target_value: this is a value is being compared. Values that are str, int, floats should all be entered as
                      strings. The parser will automatically add the '' required for string comparison for the 
                      mySQL query.
        end_parentheses: This counts the number of ")" in the parsed string, can be 0 to n_int.
                      
          
            
    Formatted output: a list with each element is a tuple with the format (spexodisks_handle, x_data_list, y_data_list).
    The list is alphabetically sorted by the spexodisks handle. x_data_list and y_data_list, will be list with a
    length of at least 1, but more are possible many more depending on how much data was found.
    The elements of x_data_list and y_data_list are NamedTuples and can be accessed using the "." or attribute 
    structure. 
    
    
    
    About the query design. I am using a reductive strategy to reduce the number of comparisons required. 
    We first create an intermediate table with the maximum amount of data but only for two data types. But now we can 
    do any number of comparisons but will already start reduced data set to search. Additionally, I standardized the 
    intermediate table format, so that subsequent conditional statements can have standard formatting.
    
    The results are fast. I am considering pre-making every possible intermediate table. Or at least saving the 
    one we already made and looking up if they exist before making another. 
    
    
    
    Plot example below for input and output examples. Run this file in Pycharm's debug mode to examine the 
    states and structure of the code as it runs. Using pycharm "run this file in the python console", to continue 
    testing on and using these variables set after the code has competed successfully. 
    
    
    """

    qe = QueryEngine()
    # test1 = qe.query(query_str="xy_plot,teff,dist")
    # test2 = qe.query(query_str="xy_plot,mass,spectrum_resolution_um")
    # test3 = qe.query(query_str="xy_plot,spectrum_min_wavelength_um,spectrum_max_wavelength_um")
    #
    # test4 = qe.query(query_str="xy_plot,mass,dist,"
    #                            "and|((|float_param_type|=|teff|  ,"
    #                            "and|  |float_value     |>|4000|) ,"
    #                            "and| (|float_param_type|=|teff|  ,"
    #                            "and|  |float_value     |<|5000|))")
    # test5 = qe.query(query_str="xy_plot,spectrum_min_wavelength_um,dist,"
    #                            "and|((|float_param_type|=|teff|  ,"
    #                            "and|  |float_value     |>|4000|) ,"
    #                            "and| (|float_param_type|=|teff|  ,"
    #                            "and|  |float_value     |<|5000|))")
    #
    # test6 = qe.query(query_str="xy_plot,spectrum_min_wavelength_um,dist,"
    #                            "and|((|float_param_type |=|teff   |  ,"
    #                            "and|  |float_value      |>|4000   |) ,"
    #                            "and| (|float_param_type |=|teff   |  ,"
    #                            "and|  |float_value      |<|5000   |)),"
    #                            "and| (|spectrum_set_type|=|creres |  ,"
    #                            "or |  |spectrum_set_type|=|nirspec|)  ")
    test7 = qe.query(query_str="3,spectrum_min_wavelength_um,teff,rings,"
                               "and|((|float_param_type |=|teff   |  ,"
                               "and|  |float_value      |>|4000   |) ,"
                               "and| (|float_param_type |=|teff   |  ,"
                               "and|  |float_value      |<|5000   |)),"
                               "and| (|spectrum_set_type|=|creres |  ,"
                               "or |  |spectrum_set_type|=|nirspec|)  ")

    for a_test in [test7]:  # test1, test2, test3, test4, test5, test6]:
        fig, ax = plt.subplots()
        x_param, y_param, x_units, y_units = None, None, None, None
        for spexodisks_handle, x_data_list, y_data_list in a_test:
            mean_x = np.mean([x_data.value for x_data in x_data_list])
            mean_y = np.mean([y_data.value for y_data in y_data_list])
            coordinate_pairs_this_object = {}
            for x_data in x_data_list:
                for y_data in y_data_list:
                    r, phi = local_polar_coordinates(x=x_data.value, y=y_data.value, x_offset=mean_x, y_offset=mean_y)
                    coordinate_pairs_this_object[(r, phi)] = (x_data, y_data)
                    if x_param is None:
                        x_param = x_data.param
                    if y_param is None:
                        y_param = y_data.param
                    if x_units is None:
                        x_units = x_data.units
                    if y_units is None:
                        y_units = y_data.units
            plot_coordinate_pairs = sorted(coordinate_pairs_this_object.keys(), key=itemgetter(1, 0))
            if len(plot_coordinate_pairs) > 2:
                plot_coordinate_pairs.append(plot_coordinate_pairs[0])
            x = []
            y = []
            for pair in plot_coordinate_pairs:
                x_data, y_data = coordinate_pairs_this_object[pair]
                x.append(x_data.value)
                y.append(y_data.value)
            plt.plot(x, y)
        plt.title(F"{len(a_test)} objects plotted")
        plt.xlabel(F"{x_param.upper()} ({x_units})")
        plt.ylabel(F"{y_param.upper()} ({y_units})")
        plt.show()


