import pytest
import os
import yaml
import logging as lg
import pandas as pd
import datetime as dt

from urbanaccess import utils
from urbanaccess import config


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
