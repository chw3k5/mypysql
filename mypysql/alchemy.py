import pandas as pd
import numpy as np
import sqlalchemy as sa

from mypysql.get_login import sql_host, sql_port, sql_database, sql_user,  sql_password


uri_base = f"mysql+pymysql://{sql_user}:{sql_password}@{sql_host}:{sql_port}/"


def is_good_num(a_float):
    if any((np.isnan(a_float), np.isinf(a_float))):
        return False
    return True


null_val = np.nan


def format_spectrum(single_spectrum):
    if single_spectrum.flux_error is None:
        for wavelength_um, velocity_kmps, flux \
                in zip(single_spectrum.wavelength_um, single_spectrum.velocity_kmps, single_spectrum.flux):
            if is_good_num(flux):
                yield wavelength_um, velocity_kmps, flux, null_val
            else:
                yield wavelength_um, velocity_kmps, null_val, null_val
    else:
        for wavelength_um, velocity_kmps, flux, flux_error \
                in zip(single_spectrum.wavelength_um, single_spectrum.velocity_kmps,
                       single_spectrum.flux, single_spectrum.flux_error):
            single_row_values = [wavelength_um, velocity_kmps]
            if is_good_num(flux):
                single_row_values.append(flux)
            else:
                single_row_values.append(null_val)
            if is_good_num(flux_error):
                single_row_values.append(flux_error)
            else:
                single_row_values.append(null_val)
            yield tuple(single_row_values)


class UploadSQL:
    def __init__(self):
        self.engine = sa.create_engine(uri_base)

    def drop_if_exists(self, table_name):
        self.engine.execute(f"DROP TABLE IF EXISTS {table_name}")

    def upload_table(self, table_name, df, schema=sql_database):
        df.to_sql(table_name, con=self.engine, schema=schema, if_exists='replace', index=False)

    def upload_spectra(self, single_spectrum, schema=sql_database, table_name=None):
        structured_array = np.array(list(format_spectrum(single_spectrum)),
                                    dtype=[('wavelength_um', '<f8'), ('velocity_kmps', '<f8'),
                                           ('flux', '<f8'), ('flux_error', '<f8')])
        df = pd.DataFrame(structured_array)
        if table_name is None:
            table_name = single_spectrum.spectrum_handle
        self.upload_table(table_name=table_name, df=df, schema=schema)
