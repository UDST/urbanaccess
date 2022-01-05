import pytest
import os
import numpy as np
import yaml
import logging as lg
import pandas as pd
import datetime as dt
import networkx as nx
from networkx.classes.multidigraph import MultiDiGraph

from urbanaccess import utils
from urbanaccess import config


@pytest.fixture
def transit_nodes():
    data = {
        'id': ['1_Lake_Stop',
               '2_12th_Stop',
               '3_19th_Stop'],
        'x': [-122.26571, -122.27085, -122.26812],
        'y': [37.79739, 37.80364, 37.80866],
        'unique_agency_id': ['rail_agency'] * 3,
        'route_type': [1] * 3,
        'stop_name': ['Lake Stop', '12th Street Stop',
                      '19th Street Stop'],
        'stop_code': ['E', 'H', 'H'],
        'net_type': ['transit'] * 3,
        'nearest_osm_node': [1, 6, 7]
    }
    index = range(3)
    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def transit_edges():
    # multi-trip edges
    data = {
        'from': ['1_Lake_Stop', '2_12th_Stop',
                 '1_Lake_Stop', '2_12th_Stop',
                 '1_Lake_Stop', '2_12th_Stop',
                 '1_Lake_Stop', '2_12th_Stop',
                 '2_12th_Stop', '3_19th_Stop',
                 '2_12th_Stop', '3_19th_Stop',
                 '2_12th_Stop', '3_19th_Stop',
                 '2_12th_Stop', '3_19th_Stop',
                 '1_Lake_Stop', '1_Lake_Stop',
                 '1_Lake_Stop', '1_Lake_Stop'],
        'to': ['2_12th_Stop', '3_19th_Stop',
               '2_12th_Stop', '3_19th_Stop',
               '2_12th_Stop', '3_19th_Stop',
               '2_12th_Stop', '3_19th_Stop',
               '2_12th_Stop', '1_Lake_Stop',
               '2_12th_Stop', '1_Lake_Stop',
               '2_12th_Stop', '1_Lake_Stop',
               '2_12th_Stop', '1_Lake_Stop',
               '3_19th_Stop', '3_19th_Stop',
               '3_19th_Stop', '3_19th_Stop'],
        'id': ['100_rail_agency_1', '100_rail_agency_2',
               '100_rail_agency_1', '100_rail_agency_2',
               '100_rail_agency_1', '100_rail_agency_2',
               '100_rail_agency_1', '100_rail_agency_2',
               '200_rail_agency_1', '200_rail_agency_2',
               '200_rail_agency_1', '200_rail_agency_2',
               '200_rail_agency_1', '200_rail_agency_2',
               '200_rail_agency_1', '200_rail_agency_2',
               '300_rail_agency_1', '300_rail_agency_1',
               '300_rail_agency_1', '300_rail_agency_1'],
        'weight': [15, 15, 15, 15, 15, 15, 15, 15,
                   15, 15, 15, 15, 15, 15, 15, 15,
                   5, 5, 5, 5],
        'unique_agency_id': ['rail_agency'] * 20,
        'unique_trip_id': ['100_rail_agency', '100_rail_agency',
                           '100_rail_agency', '100_rail_agency',
                           '100_rail_agency', '100_rail_agency',
                           '100_rail_agency', '100_rail_agency',
                           '200_rail_agency', '200_rail_agency',
                           '200_rail_agency', '200_rail_agency',
                           '200_rail_agency', '200_rail_agency',
                           '200_rail_agency', '200_rail_agency',
                           '300_rail_agency', '300_rail_agency',
                           '300_rail_agency', '300_rail_agency'],
        'sequence': [1, 2, 1, 2, 1, 2, 1, 2,
                     1, 2, 1, 2, 1, 2, 1, 2,
                     1, 1, 1, 1],
        'unique_route_id': ['A_rail_agency', 'A_rail_agency',
                            'A_rail_agency', 'A_rail_agency',
                            'A_rail_agency', 'A_rail_agency',
                            'A_rail_agency', 'A_rail_agency',
                            'A_rail_agency', 'A_rail_agency',
                            'A_rail_agency', 'A_rail_agency',
                            'A_rail_agency', 'A_rail_agency',
                            'A_rail_agency', 'A_rail_agency',
                            'B_rail_agency', 'B_rail_agency',
                            'B_rail_agency', 'B_rail_agency'],
        'net_type': ['transit'] * 20,
    }

    index = range(20)
    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def drive_nodes():
    data = {
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9],
        'x': [
            -122.265474, -122.272543, -122.273680, -122.262834, -122.269889,
            -122.271170, -122.268333, -122.266974, -122.264433],
        'y': [
            37.796897, 37.799683, 37.800206, 37.800964, 37.803884,
            37.804270, 37.809158, 37.808645, 37.807921],
        # name is not expected in OSM nodes but is used here as placeholder
        # for custom columns and as a reference for tests
        'name': [
            '1 8th & Oak', '2 8th & Franklin', '3 8th & Broadway',
            '4 14th & Oak', '5 14th & Franklin', '6 14th & Broadway',
            '7 Berkley & Broadway', '8 Berkley & Franklin',
            '9 Berkley & Harrison'],
        'net_type': ['drive'] * 9}
    index = range(9)
    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def drive_edges():
    data = {
        'from': [1, 2, 3, 6, 7, 8, 9, 4, 4, 5, 2, 5],
        'to': [2, 3, 6, 7, 8, 9, 4, 1, 5, 6, 5, 8],
        'name': ['8th', '8th', 'Broadway', 'Broadway', 'Berkley', 'Berkley',
                 'Lakeside', 'Oak', '14th', '14th', 'Franklin', 'Franklin'],
        'highway': ['residential', 'residential', 'primary', 'primary',
                    'primary', 'primary', 'residential', 'residential',
                    'primary', 'primary', 'residential', 'residential'],
        'weight': [0.3, 0.3, 0.5, 0.5, 0.6, 0.6, 1, 0.8, 0.8, 0.8, 0.4, 0.4],
        'oneway': ['yes', 'yes', 'no', 'no', 'no', 'no', 'yes', 'yes', 'no',
                   'no', 'yes', 'yes'],
        'net_type': ['drive'] * 12,
    }
    index = range(12)
    df = pd.DataFrame(data, index)

    twoway_df = df.loc[df['oneway'] == 'no']
    twoway_df.rename(columns={'from': 'to', 'to': 'from'}, inplace=True)

    edge_df = pd.concat([df, twoway_df], axis=0, ignore_index=True)
    return edge_df


@pytest.fixture
def connector_edges():
    data = {
        'from': ['1_Lake_Stop', 1, '2_12th_Stop', 6, '3_19th_Stop', 7],
        'to': [1, '1_Lake_Stop', 6, '2_12th_Stop', 7, '3_19th_Stop'],
        'weight': [0.1, 0.1, 0.2, 0.2, 0.15, 0.15],
        'net_type': ['transit to osm', 'osm to transit',
                     'transit to osm', 'osm to transit',
                     'transit to osm', 'osm to transit']}
    index = range(6)
    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def small_net(transit_edges, drive_edges, connector_edges,
              transit_nodes, drive_nodes):
    edge_df = pd.concat(
        [transit_edges, drive_edges, connector_edges], axis=0)
    node_df = pd.concat(
        [transit_nodes, drive_nodes], axis=0, ignore_index=True)

    # create final expected columns
    node_df['id_int'] = range(1, len(node_df) + 1)
    edge_df.rename(columns={'id': 'edge_id'}, inplace=True)
    tmp = pd.merge(
        edge_df, node_df[['id', 'id_int']],
        left_on='from', right_on='id', sort=False, copy=False,
        how='left')
    tmp['from_int'] = tmp['id_int']
    tmp.drop(['id_int', 'id'], axis=1, inplace=True)
    edge_df_wnumericid = pd.merge(
        tmp, node_df[['id', 'id_int']],
        left_on='to', right_on='id', sort=False, copy=False,
        how='left')
    edge_df_wnumericid['to_int'] = edge_df_wnumericid['id_int']
    edge_df_wnumericid.drop(['id_int', 'id'], axis=1, inplace=True)

    # create columns required for networkx export function
    edge_df_wnumericid['id_int'] = range(1, len(edge_df_wnumericid) + 1)

    return edge_df_wnumericid, node_df


@pytest.fixture
def df_test():
    data = {
        'col_1': [1, 2, 3],
        'col_2': ['one', 'two', 'three'],
        'col_3': [1.5, 2.5, 3.0],
    }
    index = range(3)
    df = pd.DataFrame(data, index)

    return df


@pytest.fixture
def hdf_file(tmpdir, df_test):
    hdf_file_name = 'test_hdf.h5'
    hdf_file_path = os.path.join(tmpdir.strpath, hdf_file_name)
    df_test.to_hdf(hdf_file_path, key='test_key', mode='a', format='table')
    return tmpdir.strpath, hdf_file_name


@pytest.fixture
def dictionary_to_yaml():
    return {
        'logs_folder': 'logs',
        'log_file': True,
        'log_console': False,
        'log_name': 'urbanaccess',
        'log_filename': 'urbanaccess'}


@pytest.fixture
def dictionary_to_yaml_invalid(dictionary_to_yaml):
    return [dictionary_to_yaml]


@pytest.fixture
def read_yaml_file(tmpdir, dictionary_to_yaml):
    yaml_path = os.path.join(tmpdir.strpath, 'test_yaml.yaml')
    with open(yaml_path, 'w') as f:
        yaml.dump(dictionary_to_yaml, f, default_flow_style=False)
    return tmpdir.strpath


@pytest.fixture
def read_yaml_file_invalid(tmpdir, dictionary_to_yaml_invalid):
    yaml_path = os.path.join(tmpdir.strpath, 'test_yaml.yaml')
    with open(yaml_path, 'w') as f:
        yaml.dump(dictionary_to_yaml_invalid, f,
                  default_flow_style=False)
    return tmpdir.strpath


def test_dict_to_yaml(tmpdir, dictionary_to_yaml):
    utils._dict_to_yaml(dictionary=dictionary_to_yaml,
                        yaml_dir=tmpdir.strpath,
                        yaml_name='test_yaml.yaml',
                        overwrite=True)

    yaml_path = os.path.join(tmpdir.strpath, 'test_yaml.yaml')
    with open(yaml_path, 'r') as f:
        yaml_config = yaml.safe_load(f)
    assert yaml_config == dictionary_to_yaml


def test_dict_to_yaml_invalid(tmpdir, dictionary_to_yaml,
                              dictionary_to_yaml_invalid):
    with pytest.raises(ValueError) as excinfo:
        utils._dict_to_yaml(dictionary=dictionary_to_yaml_invalid,
                            yaml_dir=tmpdir.strpath,
                            yaml_name='test_yaml.yaml',
                            overwrite=True)
    expected_error = 'Data to convert to YAML must be a dictionary.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        utils._dict_to_yaml(dictionary=dictionary_to_yaml,
                            yaml_dir=1,
                            yaml_name='test_yaml.yaml',
                            overwrite=True)
    expected_error = 'yaml directory must be a string'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        utils._dict_to_yaml(dictionary=dictionary_to_yaml,
                            yaml_dir=tmpdir.strpath,
                            yaml_name=1,
                            overwrite=True)
    expected_error = 'yaml file name must be a string'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        utils._dict_to_yaml(dictionary=dictionary_to_yaml,
                            yaml_dir=tmpdir.strpath,
                            yaml_name='test_yaml.yaml',
                            overwrite=True)
        utils._dict_to_yaml(dictionary=dictionary_to_yaml,
                            yaml_dir=tmpdir.strpath,
                            yaml_name='test_yaml.yaml',
                            overwrite=False)
    expected_error = ('test_yaml.yaml already exists. '
                      'Rename or set overwrite to True')
    assert expected_error in str(excinfo.value)


def test_dict_to_yaml_create_dir(tmpdir, dictionary_to_yaml):
    path_to_create = os.path.join(tmpdir.strpath, 'temp')
    utils._dict_to_yaml(dictionary=dictionary_to_yaml,
                        yaml_dir=path_to_create,
                        yaml_name='test_yaml.yaml',
                        overwrite=True)

    yaml_path = os.path.join(path_to_create, 'test_yaml.yaml')
    with open(yaml_path, 'r') as f:
        yaml_config = yaml.safe_load(f)
    assert yaml_config == dictionary_to_yaml


def test_yaml_to_dict(read_yaml_file, dictionary_to_yaml):
    result_dict = utils._yaml_to_dict(
        yaml_dir=read_yaml_file, yaml_name='test_yaml.yaml')
    assert sorted(result_dict) == sorted(dictionary_to_yaml)


def test_yaml_to_dict_invalid(read_yaml_file,
                              read_yaml_file_invalid):
    with pytest.raises(ValueError) as excinfo:
        result_dict = utils._yaml_to_dict(
            yaml_dir=1, yaml_name='test_yaml.yaml')
    expected_error = 'yaml directory must be a string.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result_dict = utils._yaml_to_dict(
            yaml_dir='test_dir', yaml_name='test_yaml.yaml')
    expected_error = 'test_dir does not exist or was not found.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result_dict = utils._yaml_to_dict(
            yaml_dir=read_yaml_file, yaml_name=1)
    expected_error = 'yaml file name must be a string.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result_dict = utils._yaml_to_dict(
            yaml_dir=read_yaml_file_invalid,
            yaml_name='test_yaml.yaml')
    expected_error = 'Yaml data must be a dictionary.'


def test_log_write(tmpdir):
    # test that message is written to console
    logs_folder = os.path.join(tmpdir.strpath, 'logs_test')
    log_filename = 'urbanaccess_test'
    log_name = 'urbanaccess_test_log'
    todays_date = dt.datetime.today().strftime('%Y_%m_%d')
    print_msg = 'test message'
    # set settings to a different test log folder
    config.settings.logs_folder = logs_folder
    utils.log(message=print_msg, level=lg.INFO,
              name=log_name, filename=log_filename)
    utils.log(message=print_msg, level=lg.WARNING,
              name=log_name, filename=log_filename)
    utils.log(message=print_msg, level=lg.DEBUG,
              name=log_name, filename=log_filename)
    utils.log(message=print_msg, level=lg.ERROR,
              name=log_name, filename=log_filename)
    log_file_path = os.path.join(
        logs_folder, '{}_{}.log'.format(log_filename, todays_date))

    # test that log was created and has expected path and file name
    assert os.path.exists(log_file_path) is True
    # test that level, name, and log message exist in log file
    expected_log_msg_line_1 = "INFO {} {}".format(log_name, print_msg)
    expected_log_msg_line_2 = "WARNING {} {}".format(log_name, print_msg)
    expected_log_msg_line_3 = "ERROR {} {}".format(log_name, print_msg)
    with open(log_file_path) as f:
        lines = f.readlines()
        assert expected_log_msg_line_1 in lines[0]
        assert expected_log_msg_line_2 in lines[1]
        assert expected_log_msg_line_3 in lines[2]


def test_log_print(capsys):
    # test that message is written to console
    print_msg = 'test message'
    utils.log(message=print_msg)
    # check that expected print prints
    captured = capsys.readouterr()
    assert print_msg in captured.out


def test_log_console_true():
    # test that message is written to console
    print_msg = 'test message'
    config.settings.log_console = True
    utils.log(message=print_msg)


def test_log_console_false():
    # test that message is written to console
    print_msg = 'test message'
    config.settings.log_console = False
    utils.log(message=print_msg)


def test_get_logger():
    # test with defaults (params set to None)
    logger = utils._get_logger()
    assert isinstance(logger, lg.Logger)


def test_create_hdf5_general(tmpdir):
    hdf_file_name = 'test_hdf.h5'
    hdf_file_path = tmpdir.strpath
    path = os.path.join(hdf_file_path, hdf_file_name)
    utils.create_hdf5(dir=hdf_file_path, filename=hdf_file_name,
                      overwrite_hdf5=False)
    # test file was created
    assert os.path.exists(path) is True
    # test it can be read
    with pd.HDFStore(path) as store:
        assert isinstance(store, pd.io.pytables.HDFStore)


def test_create_hdf5_overwrite_false(tmpdir):
    hdf_file_name = 'test_hdf.h5'
    hdf_file_path = tmpdir.strpath
    path = os.path.join(hdf_file_path, hdf_file_name)
    utils.create_hdf5(dir=hdf_file_path, filename=hdf_file_name,
                      overwrite_hdf5=False)
    # write the same h5 again
    utils.create_hdf5(dir=hdf_file_path, filename=hdf_file_name,
                      overwrite_hdf5=False)
    # test file was created
    assert os.path.exists(path) is True
    # test it can be read
    with pd.HDFStore(path) as store:
        assert isinstance(store, pd.io.pytables.HDFStore)


def test_create_hdf5_overwrite_true(tmpdir):
    hdf_file_name = 'test_hdf.h5'
    hdf_file_path = tmpdir.strpath
    path = os.path.join(hdf_file_path, hdf_file_name)
    utils.create_hdf5(dir=hdf_file_path, filename=hdf_file_name,
                      overwrite_hdf5=False)
    # write the same h5 again
    utils.create_hdf5(dir=hdf_file_path, filename=hdf_file_name,
                      overwrite_hdf5=True)
    # test file was created
    assert os.path.exists(path) is True
    # test it can be read
    with pd.HDFStore(path) as store:
        assert isinstance(store, pd.io.pytables.HDFStore)


def test_create_hdf5_w_defaults():
    path = os.path.join(config.settings.data_folder, 'urbanaccess.h5')
    utils.create_hdf5(dir=None, filename=None, overwrite_hdf5=False)
    # test file was created
    assert os.path.exists(path) is True
    # test it can be read
    with pd.HDFStore(path) as store:
        assert isinstance(store, pd.io.pytables.HDFStore)
    # remove test data
    os.remove(path)


def test_create_hdf5_invalid(tmpdir):
    hdf_file_name = 'test_hdf.h5'
    hdf_file_path = tmpdir.strpath
    with pytest.raises(ValueError) as excinfo:
        utils.create_hdf5(dir=1, filename=hdf_file_name,
                          overwrite_hdf5=False)
    expected_error = 'Directory must be a string.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        utils.create_hdf5(dir=hdf_file_path, filename=1,
                          overwrite_hdf5=False)
    expected_error = 'Filename must be a string.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        utils.create_hdf5(dir=hdf_file_path, filename='test_file',
                          overwrite_hdf5=False)
    expected_error = 'HDF5 filename extension must be "h5".'
    assert expected_error in str(excinfo.value)


def test_df_to_hdf5_general(tmpdir, df_test):
    hdf_file_name = 'test_hdf.h5'
    hdf_file_path = tmpdir.strpath
    path = os.path.join(hdf_file_path, hdf_file_name)
    utils.df_to_hdf5(data=df_test, key='test_key', overwrite_key=False,
                     dir=hdf_file_path, filename=hdf_file_name,
                     overwrite_hdf5=False)
    # test file was created
    assert os.path.exists(path) is True
    with pd.HDFStore(path) as store:
        result = store['test_key']
    # test df when read is as expected
    assert result.equals(df_test)


def test_df_to_hdf5_overwrite_false(tmpdir, df_test):
    hdf_file_name = 'test_hdf.h5'
    hdf_file_path = tmpdir.strpath
    path = os.path.join(hdf_file_path, hdf_file_name)
    utils.df_to_hdf5(data=df_test, key='test_key', overwrite_key=False,
                     dir=hdf_file_path, filename=hdf_file_name,
                     overwrite_hdf5=False)
    # write the same h5 again
    utils.df_to_hdf5(data=df_test, key='test_key', overwrite_key=False,
                     dir=hdf_file_path, filename=hdf_file_name,
                     overwrite_hdf5=False)
    assert os.path.exists(path) is True
    with pd.HDFStore(path) as store:
        result = store['test_key']
    # test df when read is as expected
    assert result.equals(df_test)


def test_df_to_hdf5_overwrite_true(tmpdir, df_test):
    hdf_file_name = 'test_hdf.h5'
    hdf_file_path = tmpdir.strpath
    path = os.path.join(hdf_file_path, hdf_file_name)
    utils.df_to_hdf5(data=df_test, key='test_key', overwrite_key=False,
                     dir=hdf_file_path, filename=hdf_file_name,
                     overwrite_hdf5=False)
    # write the same h5 again
    utils.df_to_hdf5(data=df_test, key='test_key', overwrite_key=True,
                     dir=hdf_file_path, filename=hdf_file_name,
                     overwrite_hdf5=False)
    assert os.path.exists(path) is True
    with pd.HDFStore(path) as store:
        result = store['test_key']
    # test df when read is as expected
    assert result.equals(df_test)


def test_hdf5_to_df(df_test, hdf_file):
    hdf_file_path, hdf_file_name = hdf_file
    result = utils.hdf5_to_df(dir=hdf_file_path, filename=hdf_file_name,
                              key='test_key')
    assert result.equals(df_test)


def test_hdf5_to_df_invalid(tmpdir, hdf_file):
    hdf_file_path, hdf_file_name = hdf_file
    with pytest.raises(ValueError) as excinfo:
        result = utils.hdf5_to_df(dir=1, filename=hdf_file_name,
                                  key='test_key')
    expected_error = 'Directory must be a string.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result = utils.hdf5_to_df(dir=hdf_file_path, filename=1,
                                  key='test_key')
    expected_error = 'Filename must be a string.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result = utils.hdf5_to_df(dir=hdf_file_path, filename='test_file',
                                  key='test_key')
    expected_error = 'HDF5 filename extension must be "h5".'
    assert expected_error in str(excinfo.value)
    hdf_file_path, hdf_file_name
    with pytest.raises(ValueError) as excinfo:
        result = utils.hdf5_to_df(dir=hdf_file_path,
                                  filename='invalid_file.h5',
                                  key='test_key')
    expected_error = 'Unable to find directory or file: {}.'
    path = os.path.join(hdf_file_path, 'invalid_file.h5')
    assert expected_error.format(path) in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result = utils.hdf5_to_df(dir=hdf_file_path,
                                  filename=hdf_file_name,
                                  key='invalid_key')
    expected_error = "Unable to find key: invalid_key. " \
                     "Keys found: ['/test_key']."
    assert expected_error in str(excinfo.value)


def test_add_unique_trip_id():
    data = {
        'trip_id': ['trip 1', 'trip 2'],
        'unique_agency_id': ['agency_a', 'agency_b'],
        'unique_trip_id': ['trip 1_agency_a', 'trip 2_agency_b']
    }
    index = range(2)
    df = pd.DataFrame(data, index)

    result = utils._add_unique_trip_id(
        df[['trip_id', 'unique_agency_id']])
    assert result.equals(df)


def test_add_unique_route_id():
    data = {
        'route_id': ['route 1', 'route 2'],
        'unique_agency_id': ['agency_a', 'agency_b'],
        'unique_route_id': ['route 1_agency_a', 'route 2_agency_b']
    }
    index = range(2)
    df = pd.DataFrame(data, index)

    result = utils._add_unique_route_id(
        df[['route_id', 'unique_agency_id']])
    assert result.equals(df)


def test_add_unique_stop_id():
    data = {
        'stop_id': ['stop 1', 'stop 2'],
        'unique_agency_id': ['agency_a', 'agency_b'],
        'unique_stop_id': ['stop 1_agency_a', 'stop 2_agency_b']
    }
    index = range(2)
    df = pd.DataFrame(data, index)

    result = utils._add_unique_stop_id(
        df[['stop_id', 'unique_agency_id']])
    assert result.equals(df)


def test_df_to_networkx(small_net):
    edge_df, node_df = small_net
    nx_graph = utils.df_to_networkx(
        nodes=node_df, edges=edge_df,
        from_id_col='from_int', to_id_col='to_int',
        edge_id_col='id_int', edge_weight_col='weight',
        node_id_col='id_int', node_x_col='x', node_y_col='y',
        graph_name='urbanaccess network',
        crs={'init': 'epsg:4326'})
    # check networkx graph type
    assert isinstance(nx_graph, MultiDiGraph)
    # check metadata
    assert nx_graph.graph['name'] == 'urbanaccess network'
    assert nx_graph.graph['crs'] == {'init': 'epsg:4326'}
    # check one col for expected data consistency
    node_df = node_df.set_index('id_int')
    expected_id_attr = node_df['id'].to_dict()
    result_id_attr = nx.get_node_attributes(nx_graph, 'id')
    assert result_id_attr == expected_id_attr
    # check one edge record for expected data consistency
    result_id_attr = nx.get_edge_attributes(nx_graph, 'id_int')
    assert result_id_attr[(1, 2, 1)] == 1


def test_df_to_networkx_invalid(small_net):
    edge_df, node_df = small_net
    with pytest.raises(ValueError) as excinfo:
        nx_graph = utils.df_to_networkx(
            nodes=node_df, edges=edge_df,
            from_id_col=None, to_id_col='to_int',
            edge_id_col='id_int', edge_weight_col='weight',
            node_id_col='id_int', node_x_col='x', node_y_col='y',
            graph_name='urbanaccess network',
            crs={'init': 'epsg:4326'})
    expected_error = 'required parameters cannot be None.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        nx_graph = utils.df_to_networkx(
            nodes=pd.DataFrame(), edges=edge_df,
            from_id_col='from_int', to_id_col='to_int',
            edge_id_col='id_int', edge_weight_col='weight',
            node_id_col='id_int', node_x_col='x', node_y_col='y',
            graph_name='urbanaccess network',
            crs={'init': 'epsg:4326'})
    expected_error = 'nodes DataFrame contains no records.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        nx_graph = utils.df_to_networkx(
            nodes=node_df, edges=pd.DataFrame(),
            from_id_col='from_int', to_id_col='to_int',
            edge_id_col='id_int', edge_weight_col='weight',
            node_id_col='id_int', node_x_col='x', node_y_col='y',
            graph_name='urbanaccess network',
            crs={'init': 'epsg:4326'})
    expected_error = 'edges DataFrame contains no records.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        nx_graph = utils.df_to_networkx(
            nodes=node_df, edges=edge_df,
            from_id_col='from_int', to_id_col='to_int',
            edge_id_col='id_int', edge_weight_col='weight',
            node_id_col='id_int', node_x_col='x', node_y_col='y',
            node_attr=['id_test'], edge_attr=None,
            graph_name='urbanaccess network',
            crs={'init': 'epsg:4326'})
    expected_error = ("nodes DataFrame missing expected column(s) passed in"
                      " 'node_attr' parameter.")
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        nx_graph = utils.df_to_networkx(
            nodes=node_df, edges=edge_df,
            from_id_col='from_int', to_id_col='to_int',
            edge_id_col='id_int', edge_weight_col='weight',
            node_id_col='id_int', node_x_col='x', node_y_col='y',
            node_attr=None, edge_attr=['id_test'],
            graph_name='urbanaccess network',
            crs={'init': 'epsg:4326'})
    expected_error = ("edges DataFrame missing expected column(s) passed in"
                      " 'edge_attr' parameter.")
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # make invalid edge df
        invalid_edges_df = edge_df.copy()
        invalid_edges_df.drop(columns=['from_int'], inplace=True)
        nx_graph = utils.df_to_networkx(
            nodes=node_df, edges=invalid_edges_df,
            from_id_col='from_int', to_id_col='to_int',
            edge_id_col='id_int', edge_weight_col='weight',
            node_id_col='id_int', node_x_col='x', node_y_col='y',
            graph_name='urbanaccess network',
            crs={'init': 'epsg:4326'})
    expected_error = ("edges DataFrame missing a required column. "
                      "Expected columns: ['from_int', 'to_int', 'id_int', "
                      "'weight'].")
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # make invalid node df
        invalid_nodes_df = node_df.copy()
        invalid_nodes_df.drop(columns=['id_int'], inplace=True)
        nx_graph = utils.df_to_networkx(
            nodes=invalid_nodes_df, edges=edge_df,
            from_id_col='from_int', to_id_col='to_int',
            edge_id_col='id_int', edge_weight_col='weight',
            node_id_col='id_int', node_x_col='x', node_y_col='y',
            graph_name='urbanaccess network',
            crs={'init': 'epsg:4326'})
    expected_error = ("nodes DataFrame missing a required column. "
                      "Expected columns: ['id_int', 'x', 'y'].")
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # make invalid edge df
        invalid_edges_df = edge_df.copy()
        invalid_edges_df['from_int'] = np.nan
        nx_graph = utils.df_to_networkx(
            nodes=node_df, edges=invalid_edges_df,
            from_id_col='from_int', to_id_col='to_int',
            edge_id_col='id_int', edge_weight_col='weight',
            node_id_col='id_int', node_x_col='x', node_y_col='y',
            graph_name='urbanaccess network',
            crs={'init': 'epsg:4326'})
    expected_error = ("edges DataFrame have nulls in required column: "
                      "from_int. No nulls allowed.")
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # make invalid node df
        invalid_nodes_df = node_df.copy()
        invalid_nodes_df['id_int'] = np.nan
        nx_graph = utils.df_to_networkx(
            nodes=invalid_nodes_df, edges=edge_df,
            from_id_col='from_int', to_id_col='to_int',
            edge_id_col='id_int', edge_weight_col='weight',
            node_id_col='id_int', node_x_col='x', node_y_col='y',
            graph_name='urbanaccess network',
            crs={'init': 'epsg:4326'})
    expected_error = ("nodes DataFrame have nulls in required column: id_int. "
                      "No nulls allowed.")
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # make invalid node df
        invalid_nodes_df = node_df.copy()
        invalid_nodes_df['id_int'] = invalid_nodes_df['id_int'] + 2
        nx_graph = utils.df_to_networkx(
            nodes=invalid_nodes_df, edges=edge_df,
            from_id_col='from_int', to_id_col='to_int',
            edge_id_col='id_int', edge_weight_col='weight',
            node_id_col='id_int', node_x_col='x', node_y_col='y',
            graph_name='urbanaccess network',
            crs={'init': 'epsg:4326'})
    expected_error = ("node IDs in from_int column in edges DataFrame do "
                      "not match node IDs in nodes DataFrame.")
    assert expected_error in str(excinfo.value)
