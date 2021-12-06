import pytest
import yaml
import os

from urbanaccess import config


@pytest.fixture
def settings_valid():
    # build current dict of configs
    return config.settings.to_dict()


@pytest.fixture
def expected_settings_dict():
    return {
        'data_folder': 'data',
        'images_folder': 'images',
        'image_filename': 'urbanaccess_plot',
        'logs_folder': 'logs',
        'log_file': True,
        'log_console': False,
        'log_name': 'urbanaccess',
        'log_filename': 'urbanaccess',
        'txt_encoding': 'utf-8',
        'gtfs_api': {
            'gtfsdataexch':
                'http://www.gtfs-data-exchange.com/api/agencies'
                '?format=csv'}}


@pytest.fixture
def valid_setting_keys():
    return sorted(['data_folder', 'images_folder', 'image_filename',
                   'logs_folder', 'log_file', 'log_console', 'log_name',
                   'log_filename', 'txt_encoding', 'gtfs_api'])


@pytest.fixture
def settings_from_yaml(tmpdir, settings_valid):
    # make 1 change to test yaml loading downstream
    settings_valid['data_folder'] = 'test'
    yaml_path = os.path.join(tmpdir.strpath, 'urbanaccess_config.yaml')
    with open(yaml_path, 'w') as f:
        yaml.dump(settings_valid, f, default_flow_style=False)
    return tmpdir.strpath


@pytest.fixture
def settings_to_yaml(tmpdir, settings_valid):
    # make 1 change to test yaml loading downstream
    config.settings.data_folder = 'test'
    return config.settings


def test_config_object():
    assert isinstance(config.settings, config.urbanaccess_config)
    assert isinstance(config.settings.to_dict(), dict)


def test_config_to_dict(expected_settings_dict):
    assert isinstance(config.settings.to_dict(), dict)
    assert config.settings.to_dict() == expected_settings_dict


def test_format_check(settings_valid):
    config._format_check(settings_valid)


def test_format_check_invalid(
        valid_setting_keys, expected_settings_dict):
    key_error_msg = ("Configuration keys: {} do not match "
                     "required keys: {}.")
    with pytest.raises(ValueError) as excinfo:
        # remove valid key txt_encoding
        invalid_dict = expected_settings_dict.copy()
        del invalid_dict['txt_encoding']
        config._format_check(invalid_dict)
    expected_error = key_error_msg.format(
        list(invalid_dict.keys()), valid_setting_keys)
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # add invalid key
        invalid_dict = expected_settings_dict.copy()
        invalid_dict.update({'invalid_key': 'test'})
        config._format_check(invalid_dict)
    expected_error = key_error_msg.format(
        list(invalid_dict.keys()), valid_setting_keys)
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        invalid_dict = expected_settings_dict.copy()
        invalid_dict['log_filename'] = 1
        config._format_check(invalid_dict)
    expected_error = "Key: 'log_filename' value must be string."
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        invalid_dict = expected_settings_dict.copy()
        invalid_dict['log_console'] = 'invalid'
        config._format_check(invalid_dict)
    expected_error = "Key: 'log_console' value must be boolean."
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        invalid_dict = expected_settings_dict.copy()
        invalid_dict['gtfs_api'] = {'invalid': 'test_url'}
        config._format_check(invalid_dict)
    expected_error = ("gtfs_api key: 'invalid' does not match valid "
                      "key(s): ['gtfsdataexch'].")
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        invalid_dict = expected_settings_dict.copy()
        invalid_dict['gtfs_api']['gtfsdataexch'] = 1
        config._format_check(invalid_dict)
    expected_error = ("gtfs_api key: 'gtfsdataexch' value must be "
                      "string.")
    assert expected_error in str(excinfo.value)


def test_from_yaml_config(settings_from_yaml):
    result_settings = config.settings.from_yaml(
        configdir=settings_from_yaml, yamlname='urbanaccess_config.yaml')
    result_settings_dict = result_settings.to_dict()
    assert result_settings_dict['data_folder'] == 'test'


def test_to_yaml_config(tmpdir, settings_to_yaml):
    yamlname = 'urbanaccess_config.yaml'
    settings_to_yaml.to_yaml(
        configdir=tmpdir.strpath,  yamlname=yamlname, overwrite=True)

    yaml_path = os.path.join(tmpdir.strpath, yamlname)
    with open(yaml_path, 'r') as f:
        yaml_config = yaml.safe_load(f)
    assert sorted(yaml_config) == sorted(settings_to_yaml.to_dict())
    assert yaml_config['data_folder'] == 'test'
