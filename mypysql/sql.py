import os
import string
import secrets
from datetime import datetime

from numpy import float32, float64
import mysql.connector

from mypysql.get_login import sql_host, sql_user, sql_database, sql_password, sql_port

from mypysql.get_tables import create_tables, dynamically_named_tables, name_specs, param_ref, max_star_name_size, \
    float_param, float_param_error, str_param, str_param_error, update_schema_map


def make_insert_columns_str(table_name, columns, database):
    insert_str = F"INSERT INTO {database}.{table_name}("
    columns_str = ""
    for column_name in columns:
        columns_str += F"`{column_name}`, "
    insert_str += columns_str[:-2] + ") VALUES"
    return insert_str


def make_insert_many_columns_str(table_name, columns, database=None):
    insert_str = make_insert_columns_str(table_name=table_name, columns=columns, database=database) + "("

    for i in range(len(columns)):
        if i == 0:
            insert_str += "%s"
        else:
            insert_str += ", %s"
    return insert_str + ")"


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
    new_configs_dir = os.path.join(dir_path, 'new_configs')
    if not os.path.isdir(new_configs_dir):
        os.mkdir(new_configs_dir)
    config_file_name = os.path.join(new_configs_dir, '../sql_config.py')
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
        self.buffers = {}
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

    def make_new_server_user(self, user_name, password=None):
        self.new_user(user_name=user_name, password=password)
        if self.host == "localhost":
            self.cursor.execute(F"""GRANT ALL PRIVILEGES ON *.* TO '{user_name}';""")
        else:
            aws_grants = "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, " + \
                         "ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, " +\
                         "REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, " + \
                         F"EVENT, TRIGGER ON *.* TO '{user_name}';"
            self.cursor.execute(aws_grants)
        self.cursor.execute(F"""FLUSH PRIVILEGES;""")
        self.connection.commit()
        print(F"Successfully granted the Server role to {user_name} for host {self.host}")

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

    def create_stars_table(self, max_summary_char_len=1000, database=None):
        table_name = 'stars'
        if database is None:
            database = sql_database
        self.open_if_closed()
        self.drop_if_exists(table_name=table_name, database=database)
        if self.verbose:
            print(f"  Creating the SQL Table: '{table_name}' in the database: {database}")
        table_str = f"CREATE TABLE `stars` (`spexodisks_handle` {name_specs}" + \
                    f"`pop_name` {name_specs}" + \
                    f"`preferred_simbad_name` {name_specs}" + \
                    f"`simbad_link` VARCHAR({max_star_name_size + 72}), " + \
                    f"`ra_dec` VARCHAR({35}), " + \
                    f"`ra_dec_ref` {param_ref}" + \
                    f"`ra` DOUBLE, " + \
                    f"`dec` DOUBLE, " + \
                    f"`esa_sky` VARCHAR({200}), " + \
                    f"`spectra_summary` VARCHAR({max_summary_char_len}) NOT NULL, " + \
                    "PRIMARY KEY (`spexodisks_handle`)) ENGINE=InnoDB;"
        self.cursor.execute(table_str)

    def create_data_stats_table(self, inst_keys, database=None):
        table_name = 'data_stats'
        if database is None:
            database = sql_database
        self.open_if_closed()
        self.drop_if_exists(table_name=table_name, database=database)
        if self.verbose:
            print(f"  Creating the SQL Table: '{table_name}' in the database: {database}")
        self.cursor.execute("USE " + database + ";")
        table_str = f"CREATE TABLE `{table_name}` (" + \
                    f"`total_stars` INT NOT NULL, " + \
                    f"`total_spectra` INT NOT NULL, " + \
                    f"`all_instruments` VARCHAR({1000}) NOT NULL, " + \
                    f"`all_instruments_names` VARCHAR({2000}) NOT NULL, "
        for inst_name in sorted(inst_keys):
            table_str += f"`{inst_name}_spectra` INT NOT NULL, "
        table_str = table_str[:-2] + ") ENGINE=InnoDB;"
        self.cursor.execute(table_str)

    def create_stats_total_table(self, database=None):
        table_name = 'stats_total'
        if database is None:
            database = sql_database
        self.open_if_closed()
        self.drop_if_exists(table_name=table_name, database=database)
        if self.verbose:
            print(f"  Creating the SQL Table: '{table_name}' in the database: {database}")
        self.cursor.execute("USE " + database + ";")
        table_str = f"CREATE TABLE `{table_name}` (" + \
                    f"`total_stars` INT NOT NULL, " + \
                    f"`total_spectra` INT NOT NULL " + \
                    f") ENGINE=InnoDB;"
        self.cursor.execute(table_str)

    def create_stats_inst_table(self, database=None):
        table_name = 'stats_instrument'
        if database is None:
            database = sql_database
        self.open_if_closed()
        self.drop_if_exists(table_name=table_name, database=database)
        if self.verbose:
            print(f"  Creating the SQL Table: '{table_name}' in the database: {database}")
        self.cursor.execute("USE " + database + ";")
        table_str = f"CREATE TABLE `{table_name}` (" + \
                    f"`order_index` INT NOT NULL AUTO_INCREMENT, " + \
                    f"`inst_handle` VARCHAR({100}) NOT NULL, " + \
                    f"`inst_name` VARCHAR({100}) NOT NULL," + \
                    f"`inst_name_short` VARCHAR({100}) NOT NULL," + \
                    f"`spectra_count` INT NOT NULL, " + \
                    "PRIMARY KEY (`order_index`)" + \
                    ") ENGINE=InnoDB;"
        self.cursor.execute(table_str)

    def create_units_table(self, database=None):
        table_name = 'available_params_and_units'
        if database is None:
            database = sql_database
        self.open_if_closed()
        self.drop_if_exists(table_name=table_name, database=database)
        if self.verbose:
            print(f"  Creating the SQL Table: '{table_name}' in the database: {database}")
        self.cursor.execute("USE " + database + ";")
        table_str = f"CREATE TABLE `{table_name}` (" + \
                    f"`order_index` INT NOT NULL AUTO_INCREMENT, " + \
                    f"`param_handle` VARCHAR({100}) NOT NULL, " + \
                    f"`units` VARCHAR({20})," + \
                    f"`short_label` VARCHAR({50})," + \
                    f"`plot_axis_label` VARCHAR({50})," + \
                    f"`for_display` TINYINT NOT NULL, " + \
                    f"`decimals` TINYINT, " + \
                    f"PRIMARY KEY (`order_index`)) ENGINE=InnoDB;"
        self.cursor.execute(table_str)

    def insert_into_table(self, table_name, data, database=None):
        if database is None:
            database = sql_database
        self.open_if_closed()
        insert_str = insert_into_table_str(table_name, data, database=database)
        self.cursor.execute(insert_str)
        self.connection.commit()

    def buffer_insert_init(self, table_name, columns, database, run_silent=False, buffer_num=0):
        if self.verbose and not run_silent:
            print("  Buffer inserting " + database + "." + table_name)
        self.buffers[buffer_num] = make_insert_columns_str(table_name, columns, database)

    def buffer_insert_value(self, values, buffer_num=0):
        self.buffers[buffer_num] += make_insert_values_str(values) + ", "

    def buffer_insert_execute(self, run_silent=False, buffer_num=0):
        self.open_if_closed()
        self.cursor.execute(self.buffers[buffer_num][:-2] + ";")
        self.connection.commit()
        if self.verbose and not run_silent:
            print("    Table inserted")

    def insert_spectrum_table(self, table_name, columns, data, database=None):
        self.creat_table(table_name=table_name,  database=database, dynamic_type='spectrum',
                         run_silent=True)
        insert_str = make_insert_many_columns_str(table_name=table_name, columns=columns, database=database)
        self.cursor.executemany(insert_str, data)
        self.connection.commit()

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

    def get_tables(self, database):
        return self.query(F'''SELECT table_name FROM information_schema.tables
                                WHERE table_schema = "{database}"''')

    def main_table(self, database):
        self.prep_table_ops(table="main", database=database)
        self.cursor.execute(
            """CREATE TABLE `main` (main_index int NOT NULL AUTO_INCREMENT, PRIMARY KEY (main_index))
                     SELECT stars.spexodisks_handle, stars.pop_name, stars.preferred_simbad_name, 
                            object_params_str.str_index_params, object_params_str.str_param_type, 
                            object_params_str.str_value, object_params_str.str_error, object_params_str.str_ref, 
                            object_params_str.str_units, object_params_str.str_notes,
                            object_params_float.float_index_params, object_params_float.float_param_type, 
                            object_params_float.float_value, object_params_float.float_error_low, 
                            object_params_float.float_error_high, object_params_float.float_ref, 
                            object_params_float.float_units, object_params_float.float_notes,
                            spectra.spectrum_handle, spectra.spectrum_set_type, spectra.spectrum_observation_date, 
                            spectra.spectrum_pi, spectra.spectrum_reference, spectra.spectrum_downloadable, 
                            spectra.spectrum_data_reduction_by, spectra.spectrum_aor_key, 
                            spectra.spectrum_flux_is_calibrated, spectra.spectrum_ref_frame, 
                            spectra.spectrum_min_wavelength_um, spectra.spectrum_max_wavelength_um, 
                            spectra.spectrum_resolution_um, spectra.spectrum_output_filename
                     FROM stars
                         LEFT OUTER JOIN object_params_str
                             ON stars.spexodisks_handle = object_params_str.spexodisks_handle
                         LEFT OUTER JOIN object_params_float
                             ON stars.spexodisks_handle = object_params_float.spexodisks_handle
                         LEFT OUTER JOIN spectra
                             ON stars.spexodisks_handle = spectra.spexodisks_handle""")
        self.connection.commit()

    def params_tables(self, database):
        table_name = "available_float_params"
        self.prep_table_ops(table=F"{table_name}", database=database)
        self.cursor.execute(F"""CREATE TABLE {table_name}
                                    SELECT DISTINCT(float_param_type) AS float_params FROM object_params_float;""")
        self.connection.commit()

        table_name = "available_str_params"
        self.prep_table_ops(table=F"{table_name}", database=database)
        self.cursor.execute(F"""CREATE TABLE {table_name}
                                    SELECT DISTINCT(str_param_type) AS str_params FROM object_params_str;""")
        self.connection.commit()

        table_name = "available_spectrum_params"
        self.prep_table_ops(table=F"{table_name}", database=database)
        self.cursor.execute(F"""CREATE TABLE {table_name}
                                    SELECT COLUMN_NAME AS spectrum_params
                                        FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'spectra';""")
        self.connection.commit()

    def handles_table(self, database):
        table_name = "handles"
        self.prep_table_ops(table=F"{table_name}", database=database)
        self.cursor.execute(F"""CREATE TABLE {database}.{table_name}
                                    SELECT {database}.spectra.spectrum_handle, {database}.stars.spexodisks_handle,
                                        {database}.stars.pop_name, {database}.stars.preferred_simbad_name
                                    FROM {database}.stars 
                                        LEFT OUTER JOIN {database}.spectra
                                        ON {database}.stars.spexodisks_handle = {database}.spectra.spexodisks_handle;""")
        self.cursor.execute(F"""ALTER TABLE {database}.{table_name};""")
        self.connection.commit()

    def create_curated_table(self, float_params, str_params, database):
        float_specs = [float_param.replace(' NOT NULL', ''), float_param_error, float_param_error, param_ref]
        str_specs = [str_param.replace(' NOT NULL', ''), str_param_error, str_param_error, param_ref]
        table_name = 'curated'

        self.prep_table_ops(table=table_name, database=database)
        create_str = F"CREATE TABLE `{table_name}` (`spexodisks_handle` " + name_specs + "`pop_name` " + name_specs + \
                     "`preferred_simbad_name` " + name_specs + \
                     f"`simbad_link` VARCHAR({max_star_name_size + 72}), " + \
                     f"`ra_dec` VARCHAR({35}), " + \
                     f"`ra` VARCHAR({18}), " + \
                     f"`dec` VARCHAR({18}), " + \
                     f"`esa_sky` VARCHAR({200}), " + \
                     f"`has_spectra` TINYINT NOT NULL, "
        all_column_names = ['spexodisks_handle', 'pop_name', 'preferred_simbad_name']
        for param in float_params:
            column_names = [F"{param}_value", F"{param}_err_low", F"{param}_err_high", F"{param}_ref"]
            all_column_names.extend(column_names)
            for column_name, specs in zip(column_names, float_specs):
                create_str += F"`{column_name}` {specs}"
                if column_name in {'is_jwst_target_value', 'is_dsharp_target_value'}:
                    create_str = create_str[:-2] + ' NULL DEFAULT 0, '
        for a_str_param in str_params:
            str_column_names = [F"{a_str_param}_value", F"{a_str_param}_err_low",
                                F"{a_str_param}_err_high", F"{a_str_param}_ref"]
            all_column_names.extend(str_column_names)
            for column_name, spec in zip(str_column_names, str_specs):
                create_str += F"`{column_name}` {spec}"
        create_str += "PRIMARY KEY (`spexodisks_handle`)" + ") ENGINE=InnoDB;"
        self.cursor.execute(create_str)
        self.connection.commit()
        return all_column_names

    def update_schemas(self):
        for target, source in update_schema_map:
            # get all the tables in the source schema
            source_tables = [one_ple[0] for one_ple in self.get_tables(database=source)]
            for table_name in source_tables:
                # drop the old database table
                self.drop_if_exists(table_name=table_name, database=target)
                # move the new database tables to the old locations
                self.cursor.execute(F"""ALTER TABLE {source}.{table_name}
                                        RENAME {target}.{table_name};""")
                print(F"     Moved {source}.{table_name} to {target}.{table_name}")
                self.connection.commit()
            print(F"Tables updated in {target}\n   {source_tables}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == "__main__":
    with OutputSQL() as output_sql:
        # output_sql.update_schemas()
        output_sql.make_new_server_user('hussain')