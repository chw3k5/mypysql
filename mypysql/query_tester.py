import numpy as np
from operator import itemgetter

from mypysql.query import QueryEngine
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use(backend="TkAgg")


def local_polar_coordinates(x, y, x_offset, y_offset):
    x_local = x - x_offset
    y_local = y - y_offset
    r = np.sqrt((x_local ** 2.0) + (y_local ** 2.0))
    phi = np.arctan2(x_local, y_local)
    return r, phi


def plot_plot_data(a_test):
    fig, ax = plt.subplots()
    x_param, y_param, x_units, y_units = None, None, None, None
    for spexodisks_handle, _spec_h, _sed_h, _pop, _simbad, x_data_list, y_data_list in a_test:
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


qe = QueryEngine()
qe2 = QueryEngine()
test1 = qe.query(query_str="plot,2,teff,dist")
test2 = qe2.query(query_str="plot,2,mass,spectrum_resolution_um")
test3 = qe2.query(query_str="plot,2,spectrum_min_wavelength_um,spectrum_max_wavelength_um")

test4 = qe.query(query_str="plot,2,mass,dist,"
                           "and|((|float_param_type|=|teff|  ,"
                           "and|  |float_value     |>|4000|) ,"
                           "and| (|float_param_type|=|teff|  ,"
                           "and|  |float_value     |<|5000|))")
test5 = qe.query(query_str="plot,2,spectrum_min_wavelength_um,dist,"
                           "and|((|float_param_type|=|teff|  ,"
                           "and|  |float_value     |>|4000|) ,"
                           "and| (|float_param_type|=|teff|  ,"
                           "and|  |float_value     |<|5000|))")

test6 = qe.query(query_str="plot,2,spectrum_min_wavelength_um,dist,"
                           "and|((|float_param_type |=|teff   |  ,"
                           "and|  |float_value      |>|4000   |) ,"
                           "and| (|float_param_type |=|teff   |  ,"
                           "and|  |float_value      |<|5000   |)),"
                           "and| (|spectrum_set_type|=|creres |  ,"
                           "or |  |spectrum_set_type|=|nirspec|)  ")
test7 = qe2.query(query_str="plot,4,spectrum_min_wavelength_um,teff,rings,dist,"
                            "and|((|float_param_type |=|teff   |  ,"
                            "and|  |float_value      |>|4000   |) ,"
                            "and| (|float_param_type |=|teff   |  ,"
                            "and|  |float_value      |<|5000   |)),"
                            "and| (|spectrum_set_type|=|creres |  ,"
                            "or |  |spectrum_set_type|=|nirspec|)  ")

test8 = qe.query(query_str="table,4,spectrum_min_wavelength_um,teff,rings,dist,"
                           "and|((|float_param_type |=|teff   |  ,"
                           "and|  |float_value      |>|4000   |) ,"
                           "and| (|float_param_type |=|teff   |  ,"
                           "and|  |float_value      |<|5000   |)),"
                           "and| (|spectrum_set_type|=|creres |  ,"
                           "or |  |spectrum_set_type|=|nirspec|)  ")

curated1 = qe.curated_query(params=None, database='spexodisks')
curated2 = qe.curated_query(params=['mass', 'teff', 'dist'], database='spexodisks')
qe.close()
qe2.close()

print(QueryEngine.query_log)

for a_test in [test1, test2, test3, test4, test5, test6]:
    plot_plot_data(a_test)
