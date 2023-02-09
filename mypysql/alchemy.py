from typing import List, Optional

import numpy as np
import pandas as pd
import sqlalchemy as sa

from mypysql.get_login import sql_host, sql_port, sql_database, sql_user,  sql_password


uri_base = f"mysql+pymysql://{sql_user}:{sql_password}@{sql_host}:{sql_port}/"


def is_good_num(a_float):
    if any((np.isnan(a_float), np.isinf(a_float))):
        return False
    return True


null_val = np.nan


bandwidth_fraction_for_null_default = 0.01


def remove_bad_nums(wavelength_um: List[float], flux: List[float], flux_error: Optional[List[float]] = None):
    if flux_error is None:
        flux_error = [null_val] * len(flux)
    for wavelength_um, flux, flux_error in zip(wavelength_um, flux, flux_error):
        if is_good_num(flux):
            if not is_good_num(flux_error):
                flux_error = null_val
            yield wavelength_um, flux, flux_error


def format_spectrum(wavelength_um: List[float], flux: List[float], flux_error: Optional[List[float]] = None,
                    bandwidth_fraction_for_null: float = bandwidth_fraction_for_null_default):
    # replacement here to save memory in the function call
    wavelength_um, flux, flux_error = zip(*remove_bad_nums(wavelength_um, flux, flux_error))
    wavelength_um = np.array(wavelength_um)
    flux = np.array(flux)
    flux_error = np.array(flux_error)
    wavelength_um_min = np.min(wavelength_um)
    wavelength_um_max = np.max(wavelength_um)
    bandwidth = wavelength_um_max - wavelength_um_min
    bandwidth_for_null = bandwidth * bandwidth_fraction_for_null
    wavelength_um_step = wavelength_um[1:] - wavelength_um[:-1]
    # for large steps in wavelength, insert a null value to break up the spectrum into segments of contiguous data
    insert_count = 0
    for i, wavelength_um_step in enumerate(wavelength_um_step):
        if wavelength_um_step > bandwidth_for_null:
            insert_count += 1
            wavelength_um_null = wavelength_um[i] + (wavelength_um_step / 2.0)
            insert_index = i + insert_count
            wavelength_um = np.insert(wavelength_um, insert_index, wavelength_um_null)
            flux = np.insert(flux, insert_index, null_val)
            flux_error = np.insert(flux_error, insert_index, null_val)
    zipped_spectrum = list(zip(wavelength_um, flux, flux_error))
    structured_array = np.array(zipped_spectrum,
                                dtype=[('wavelength_um', '<f8'), ('flux', '<f8'), ('flux_error', '<f8')])
    return structured_array


class UploadSQL:
    def __init__(self):
        self.engine = sa.create_engine(uri_base)

    def drop_if_exists(self, table_name):
        self.engine.execute(f"DROP TABLE IF EXISTS {table_name}")

    def upload_table(self, table_name, df, schema=sql_database):
        df.to_sql(table_name, con=self.engine, schema=schema, if_exists='replace', index=False)

    def upload_spectra(self, table_name: str, wavelength_um: List[float], flux: List[float],
                       flux_error: Optional[List[float]] = None, schema: str = sql_database,
                       bandwidth_fraction_for_null: float = bandwidth_fraction_for_null_default):
        structured_array = format_spectrum(wavelength_um=wavelength_um, flux=flux, flux_error=flux_error,
                                           bandwidth_fraction_for_null=bandwidth_fraction_for_null)
        df = pd.DataFrame(structured_array)
        self.upload_table(table_name=table_name, df=df, schema=schema)
