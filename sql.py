import os
import secrets
import string
from datetime import datetime
from numpy import float32, float64
import mysql.connector
from mypysql.sql_config import sql_host, sql_user, sql_database, sql_password, sql_port
# # Avoid a requirement to have created a SQL config file created to run the rest of the project
# try:
#     from mypysql.sql_config import sql_host, sql_user, sql_database, sql_password, sql_port
# except ModuleNotFoundError:
#     ql_host, sql_user, sql_database, sql_password, sql_port = "", "", "", "", 3306

# quickly change the lengths of some MySQL columns
max_star_name_size = 50
max_param_type_len = 20
max_len_str_param = 100
max_ref_len = 50
max_units_len = 10
max_notes_len = 100

max_spectral_handle_len = 100
max_output_filename_len = 200

name_specs = F"VARCHAR({max_star_name_size}) NOT NULL, "
param_name = F"VARCHAR({max_param_type_len}) NOT NULL, "
float_param = F"DOUBLE NOT NULL, "
float_param_error = F"DOUBLE, "
str_param = F"VARCHAR({max_len_str_param}) NOT NULL, "
str_param_error = F"VARCHAR({max_len_str_param}), "
param_ref = F"VARCHAR({max_ref_len}), "
param_units = F"VARCHAR({max_units_len}), "
param_notes = F"VARCHAR({max_notes_len}), "

spectrum_handle = F"VARCHAR({max_spectral_handle_len}) NOT NULL, "
stacked_line_handle = F"VARCHAR({max_spectral_handle_len + 30}) NOT NULL, "
output_filename = F"VARCHAR({max_output_filename_len}), "

create_tables = {'stars': "CREATE TABLE `stars` (`spexodisks_handle` " + name_specs +
                                                "`pop_name` " + name_specs +
                                                "`preferred_simbad_name` " + name_specs +
                                                "PRIMARY KEY (`spexodisks_handle`)" +
                                                ") " +
                          "ENGINE=InnoDB;",
                 'object_name_aliases': "CREATE TABLE `object_name_aliases` (`alias` " + name_specs +
                                                                            "`spexodisks_handle` " + name_specs +
                                                                            "PRIMARY KEY (`alias`)" +
                                                                            ") " +
                                        "ENGINE=InnoDB;",
                 "object_params_str": "CREATE TABLE `object_params_str` "
                                        "(`str_index_params` int(11) NOT NULL AUTO_INCREMENT," +
                                         "`spexodisks_handle` " + name_specs +
                                         "`str_param_type` " + param_name +
                                         "`str_value` " + str_param +
                                         "`str_error` " + str_param_error +
                                         "`str_ref` " + param_ref +
                                         "`str_units` " + param_units +
                                         "`str_notes` " + param_notes +
                                         "PRIMARY KEY (`str_index_params`)" +
                                         ") " +
                                        "ENGINE=InnoDB;",
                 "object_params_float": "CREATE TABLE `object_params_float` "
                                                   "(`float_index_params` int(11) NOT NULL AUTO_INCREMENT," +
                                                    "`spexodisks_handle` " + name_specs +
                                                    "`float_param_type` " + param_name +
                                                    "`float_value` " + str_param +
                                                    "`float_error_low` " + str_param_error +
                                                    "`float_error_high` " + str_param_error +
                                                    "`float_ref` " + param_ref +
                                                    "`float_units` " + param_units +
                                                    "`float_notes` " + param_notes +
                                                    "PRIMARY KEY (`float_index_params`)" +
                                                    ") " +
                                                   "ENGINE=InnoDB;",
                 "spectra": "CREATE TABLE `spectra` (`spectrum_handle` " + spectrum_handle +
                                                    "`spexodisks_handle` " + name_specs +
                                                    "`spectrum_set_type` VARCHAR(20), " +
                                                    "`spectrum_observation_date` DATETIME, " +
                                                    "`spectrum_pi` VARCHAR(50), " +
                                                    "`spectrum_reference` " + param_ref +
                                                    "`spectrum_downloadable` TINYINT, " +
                                                    "`spectrum_data_reduction_by` VARCHAR(50), " +
                                                    "`spectrum_aor_key` INT, " +
                                                    "`spectrum_flux_is_calibrated` TINYINT, " +
                                                    "`spectrum_ref_frame` VARCHAR(20), " +
                                                    "`spectrum_min_wavelength_um` DOUBLE, " +
                                                    "`spectrum_max_wavelength_um` DOUBLE," +
                                                    "`spectrum_resolution_um` DOUBLE, " +
                                                    "`spectrum_output_filename` " + output_filename +
                                                    "PRIMARY KEY (`spectrum_handle`)" +
                                                    ") " +
                                                   "ENGINE=InnoDB;",
                 "flux_calibration": "CREATE TABLE `flux_calibration` " +
                                     "(`index_flux_cal` int(11) NOT NULL AUTO_INCREMENT, " +
                                     "`spectrum_handle` " + spectrum_handle +
                                     "`flux_cal` " + float_param +
                                     "`flux_cal_error` " + float_param_error +
                                     "`wavelength_um` " + float_param +
                                     "`ref` " + param_ref +
                                      "PRIMARY KEY (`index_flux_cal`)" +
                                      ") " +
                                     "ENGINE=InnoDB;",
                 "CO": "CREATE TABLE `CO` (`index_CO` int(11) NOT NULL AUTO_INCREMENT, " +
                                          "`wavelength_um` " + float_param +
                                          "`isotopologue` VARCHAR(20), " +
                                          "`upper_level` VARCHAR(30), " +
                                          "`lower_level` VARCHAR(30), " +
                                          "`transition` VARCHAR(30), " +
                                          "`einstein_A` " + float_param +
                                          "`upper_level_energy` " + float_param +
                                          "`lower_level_energy` " + float_param +
                                          "`g_statistical_weight_upper_level` " + float_param +
                                          "`g_statistical_weight_lower_level` " + float_param +
                                          "`upper_vibrational` INT(2), " +
                                          "`upper_rotational` INT(2), " +
                                          "`branch` VARCHAR(1), " +
                                          "`lower_vibrational` INT(2), " +
                                          "`lower_rotational` INT(2), " +
                                           "PRIMARY KEY (`index_CO`)" +
                                          ") " +
                                         "ENGINE=InnoDB;",
                 "H2O": "CREATE TABLE `H2O` (`index_H2O` int(11) NOT NULL AUTO_INCREMENT, " +
                                            "`wavelength_um` " + float_param +
                                            "`isotopologue` VARCHAR(20), " +
                                            "`upper_level` VARCHAR(30), " +
                                            "`lower_level` VARCHAR(30), " +
                                            "`transition` VARCHAR(65), " +
                                            "`einstein_A` " + float_param +
                                            "`upper_level_energy` " + float_param +
                                            "`lower_level_energy` " + float_param +
                                            "`g_statistical_weight_upper_level` " + float_param +
                                            "`g_statistical_weight_lower_level` " + float_param +
                                            "`upper_vibrational1` INT(2), " +
                                            "`upper_vibrational2` INT(2), " +
                                            "`upper_vibrational3` INT(2), " +
                                            "`upper_rotational` INT(2), " +
                                            "`upper_ka` INT(2), " +
                                            "`upper_kc` INT(2), " +
                                            "`lower_vibrational1` INT(2), " +
                                            "`lower_vibrational2` INT(2), " +
                                            "`lower_vibrational3` INT(2), " +
                                            "`lower_rotational` INT(2), " +
                                            "`lower_ka` INT(2), " +
                                            "`lower_kc` INT(2), " +
                                             "PRIMARY KEY (`index_H2O`)" +
                                            ") " +
                                           "ENGINE=InnoDB;",
                 "line_fluxes_CO": "CREATE TABLE `line_fluxes_CO` " +
                                   "(`index_CO` int(11) NOT NULL AUTO_INCREMENT, " +
                                    "`flux` " + float_param +
                                    "`flux_error` " + float_param_error +
                                    "`match_wavelength_um` " + float_param +
                                    "`wavelength_um` " + float_param +
                                    "`spectrum_handle` " + spectrum_handle +
                                    "`isotopologue` VARCHAR(20), " +
                                    "`upper_level` VARCHAR(30), " +
                                    "`lower_level` VARCHAR(30), " +
                                    "`transition` VARCHAR(30), " +
                                    "`einstein_A` " + float_param +
                                    "`upper_level_energy` " + float_param +
                                    "`lower_level_energy` " + float_param +
                                    "`g_statistical_weight_upper_level` " + float_param +
                                    "`g_statistical_weight_lower_level` " + float_param +
                                    "`upper_vibrational` INT(2), " +
                                    "`upper_rotational` INT(2), " +
                                    "`branch` VARCHAR(1), " +
                                    "`lower_vibrational` INT(2), " +
                                    "`lower_rotational` INT(2), " +
                                     "PRIMARY KEY (`index_CO`)" +
                                    ") " +
                                  "ENGINE=InnoDB;",
                 "stacked_line_spectra": "CREATE TABLE `stacked_line_spectra` " +
                                         "(`stack_line_handle` " + stacked_line_handle +
                                          "`spectrum_handle` " + spectrum_handle +
                                          "`spexodisks_handle` " + name_specs +
                                          "`transition` VARCHAR(30) NOT NULL, " +
                                          "`isotopologue` VARCHAR(20) NOT NULL, " +
                                          "`molecule` VARCHAR(20) NOT NULL, " +
                                          "PRIMARY KEY (`stack_line_handle`)" +
                                          ") " +
                                         "ENGINE=InnoDB;",
                 }

dynamically_named_tables = {"spectrum": "(`wavelength_um` " + float_param +
                                         "`velocity_kmps` " + float_param +
                                         "`flux` " + float_param_error +
                                         "`flux_error` " + float_param_error +
                                          "PRIMARY KEY (`wavelength_um`)" +
                                         ") " +
                                        "ENGINE=InnoDB;",
                            "stacked_spectrum": "(`velocity_kmps` " + float_param +
                                                 "`flux` " + float_param_error +
                                                 "`flux_error` " + float_param_error +
                                                 "PRIMARY KEY (`velocity_kmps`)" +
                                                 ") " +
                                                 "ENGINE=InnoDB;",
                            }


def make_insert_columns_str(table_name, columns, database):
    insert_str = F"INSERT INTO {database}.{table_name}("
    columns_str = ""
    for column_name in columns:
        columns_str += F"{column_name}, "
    insert_str += columns_str[:-2] + ") VALUES"
    return insert_str


def make_insert_values_str(values):
    values_str = ""
    for value in values:
        if isinstance(value, str):
            values_str += F"'{value}', "
        elif isinstance(value, (float, int, float32, float64)):
            values_str += F"{value}, "
        elif isinstance(value, bool):
            values_str += F"{int(value)}, "
        elif isinstance(value, datetime):
            values_str += F"'{str(value)}', "
        elif value is None:
            values_str += "NULL, "
        else:
            raise TypeError
    return "(" + values_str[:-2] + ")"


def insert_into_table_str(table_name, data, database=None):
    if database is None:
        database = sql_database
    columns = []
    values = []
    for column_name in sorted(data.keys()):
        columns.append(column_name)
        values.append(data[column_name])
    insert_str = make_insert_columns_str(table_name, columns, database)
    insert_str += make_insert_values_str(values) + ";"
    return insert_str


def generate_sql_config_file(user_name, password):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_file_name = os.path.join(dir_path, 'new_configs', 'sql_config.py')
    with open(config_file_name, 'w') as f:
        f.write(F"""sql_host = "{sql_host}"\n""")
        f.write(F"""sql_port = "{sql_port}"\n""")
        f.write(F"""sql_database = "{sql_database}"\n""")
        f.write(F"""sql_user = "{user_name}"\n""")
        f.write(F"""sql_password = '''{password}'''\n""")
    print(F"New sql_config.py file at to: {config_file_name}")
    print(F"For user: {user_name}")


class OutputSQL:
    def __init__(self, auto_connect=True, verbose=True):
        self.verbose = verbose
        self.host = sql_host
        self.user = sql_user
        self.port = sql_port
        self.password = sql_password
        if auto_connect:
            self.open()
        else:
            self.connection = None
            self.cursor = None
        self.buffer = None
        self.next_user_table_number = 1

    def open(self):
        if self.verbose:
            print("  Opening connection to the SQL Host Server:", sql_host)
            print("  under the user:", sql_user)
        self.connection = mysql.connector.connect(host=self.host,
                                                  user=self.user,
                                                  port=self.port,
                                                  password=self.password)
        self.cursor = self.connection.cursor()
        if self.verbose:
            print("    Connection established")

    def close(self):
        if self.verbose:
            print("  Closing SQL connection SQL Server.")
        self.cursor.close()
        self.connection.close()
        self.connection = None
        self.cursor = None
        if self.verbose:
            print("    Connection Closed")

    def open_if_closed(self):
        if self.connection is None:
            self.open()

    def new_user(self, user_name, password=None):
        if password is None:
            alphabet = string.ascii_letters + string.digits
            password = ''.join(secrets.choice(alphabet) for i in range(20))
        generate_sql_config_file(user_name=user_name, password=password)
        self.cursor.execute(F"""CREATE USER '{user_name}' IDENTIFIED BY '{password}';""")
        self.connection.commit()
        print(F"Successfully created the user {user_name} for host {self.host}")

    def make_new_super_user(self, user_name, password=None):
        self.new_user(user_name=user_name, password=password)
        self.cursor.execute(F"""GRANT ALL PRIVILEGES ON *.* TO '{user_name}';""")
        self.cursor.execute(F"""FLUSH PRIVILEGES;""")
        self.connection.commit()
        print(F"Successfully granted super user privileges to {user_name} for host {self.host}")

    def make_new_dba_user(self, user_name, password=None):
        self.new_user(user_name=user_name, password=password)
        if self.host == "localhost":
            self.cursor.execute(F"""GRANT ALL PRIVILEGES ON *.* TO '{user_name}' WITH GRANT OPTION;""")
        else:
            aws_grants = "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, " + \
                         "ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, " +\
                         "REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, " + \
                         F"EVENT, TRIGGER ON *.* TO '{user_name}' WITH GRANT OPTION;"
            self.cursor.execute(aws_grants)
        self.cursor.execute(F"""FLUSH PRIVILEGES;""")
        self.connection.commit()
        print(F"Successfully granted the Database Administrator (DBA) role to {user_name} for host {self.host}")

    def drop_if_exists(self, table_name, database=None, run_silent=False):
        if self.verbose and not run_silent:
            print("    Dropping (deleting if the table exists) the Table:", table_name)
        if database is None:
            database = sql_database
        self.cursor.execute(F"DROP TABLE IF EXISTS {database}.{table_name};")

    def creat_table(self, table_name, database=None, dynamic_type=None, run_silent=False):
        if database is None:
            database = sql_database
        self.open_if_closed()
        self.drop_if_exists(table_name=table_name, database=database, run_silent=run_silent)
        if self.verbose and not run_silent:
            print("  Creating the SQL Table: '" + table_name + "' in the database: " + database)
        self.cursor.execute("USE " + database + ";")
        if dynamic_type is None:
            table_str = create_tables[table_name]
        else:
            table_str = "CREATE TABLE `" + table_name + "` " + dynamically_named_tables[dynamic_type]
        self.cursor.execute(table_str)

    def insert_into_table(self, table_name, data, database=None):
        if database is None:
            database = sql_database
        self.open_if_closed()
        insert_str = insert_into_table_str(table_name, data, database=database)
        self.cursor.execute(insert_str)
        self.connection.commit()

    def buffer_insert_init(self, table_name, columns, database, run_silent=False):
        if self.verbose and not run_silent:
            print("  Buffer inserting " + database + "." + table_name)
        self.buffer = make_insert_columns_str(table_name, columns, database)

    def buffer_insert_value(self, values):
        self.buffer += make_insert_values_str(values) + ", "

    def buffer_insert_execute(self, run_silent=False):
        self.open_if_closed()
        self.cursor.execute(self.buffer[:-2] + ";")
        self.connection.commit()
        if self.verbose and not run_silent:
            print("    Table inserted")

    def creat_database(self, database):
        if self.verbose:
            print("  Creating the SQL Database: '" + database + "'.")
        self.open_if_closed()
        self.cursor.execute("CREATE DATABASE `" + database + "`;")

    def drop_database(self, database):
        if self.verbose:
            print("    Dropping (deleting if the database exists) the Database:", database)
        self.open_if_closed()
        self.cursor.execute("DROP DATABASE IF EXISTS `" + database + "`;")

    def clear_database(self, database):
        self.drop_database(database=database)
        self.creat_database(database=database)

    def query(self, sql_query_str):
        self.cursor.execute(sql_query_str)
        return [item for item in self.cursor]

    def prep_table_ops(self, table, database=sql_database):
        self.cursor.execute(F"""USE {database};""")
        self.cursor.execute(F"""DROP TABLE IF EXISTS `{table}`;""")

    def user_table(self, table_str, user_table_name=None, skip_if_exists=True):
        if user_table_name is None:
            user_table_name = F"user_table_{'%04i' % self.next_user_table_number}"
            self.next_user_table_number += 1
        if skip_if_exists:
            self.cursor.execute(F"""USE temp;""")
        else:
            self.prep_table_ops(table=user_table_name, database='temp')
        create_str = F"""CREATE TABLE IF NOT EXISTS `{user_table_name}` {table_str};"""
        self.cursor.execute(create_str)
        self.connection.commit()
        return user_table_name

    def main_table(self):
        self.prep_table_ops(table="main")
        self.cursor.execute(
            """CREATE TABLE `main` (main_index int NOT NULL AUTO_INCREMENT, PRIMARY KEY (main_index))
                     SELECT stars.spexodisks_handle, stars.pop_name, stars.preferred_simbad_name, 
                            object_params_str.str_index_params, object_params_str.str_param_type, object_params_str.str_value, object_params_str.str_error, object_params_str.str_ref, object_params_str.str_units, object_params_str.str_notes,
                            object_params_float.float_index_params, object_params_float.float_param_type, object_params_float.float_value, object_params_float.float_error_low, object_params_float.float_error_high, object_params_float.float_ref, object_params_float.float_units, object_params_float.float_notes,
                            spectra.spectrum_handle, spectra.spectrum_set_type, spectra.spectrum_observation_date, spectra.spectrum_pi, spectra.spectrum_reference, spectra.spectrum_downloadable, spectra.spectrum_data_reduction_by, spectra.spectrum_aor_key, spectra.spectrum_flux_is_calibrated, spectra.spectrum_ref_frame, spectra.spectrum_min_wavelength_um, spectra.spectrum_max_wavelength_um, spectra.spectrum_resolution_um, spectra.spectrum_output_filename
                     FROM stars
                         LEFT OUTER JOIN object_params_str
                             ON stars.spexodisks_handle = object_params_str.spexodisks_handle
                         LEFT OUTER JOIN object_params_float
                             ON stars.spexodisks_handle = object_params_float.spexodisks_handle
                         LEFT OUTER JOIN spectra
                             ON stars.spexodisks_handle = spectra.spexodisks_handle""")
        self.connection.commit()

    def params_tables(self):
        table_name = "available_float_params"
        self.prep_table_ops(table=F"{table_name}")
        self.cursor.execute(F"""CREATE TABLE {table_name}
                                    SELECT DISTINCT(float_param_type) AS float_params FROM object_params_float;""")
        self.connection.commit()

        table_name = "available_str_params"
        self.prep_table_ops(table=F"{table_name}")
        self.cursor.execute(F"""CREATE TABLE {table_name}
                                    SELECT DISTINCT(str_param_type) AS str_params FROM object_params_str;""")
        self.connection.commit()

        table_name = "available_spectrum_params"
        self.prep_table_ops(table=F"{table_name}")
        self.cursor.execute(F"""CREATE TABLE {table_name}
                                    SELECT COLUMN_NAME AS spectrum_params
                                        FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'spectra';""")
        self.connection.commit()

    def handles_table(self):
        table_name = "handles"
        self.prep_table_ops(table=F"{table_name}")
        self.cursor.execute(F"""CREATE TABLE spexodisks.{table_name}
                                SELECT spexodisks.spectra.spectrum_handle, spexodisks.spectra.spexodisks_handle,
                                spexodisks.stars.pop_name, spexodisks.stars.preferred_simbad_name
                                FROM spexodisks.stars 
                                LEFT OUTER JOIN spexodisks.spectra
                                ON spexodisks.stars.spexodisks_handle = spexodisks.spectra.spexodisks_handle;""")
        self.cursor.execute(F"""ALTER TABLE spexodisks.{table_name};""")
        self.connection.commit()


if __name__ == "__main__":
    output_sql = OutputSQL()
    try:
        output_sql.handles_table()
    except:
        output_sql.close()
        raise
    output_sql.close()
