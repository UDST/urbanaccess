import pytest

import urbanaccess.gtfs.utils_validation as utils_validation

def test_check_time_range_format():
    utils_validation._check_time_range_format(['07:00:00', '10:00:00'])


def test_check_time_range_format_invalid_params():
    msg = ('starttime and endtime are not in the correct format. '
           'Format should be a 24 hour clock in the following format: '
           '08:00:00 or 17:00:00.')
    with pytest.raises(ValueError) as excinfo:
        utils_validation._check_time_range_format(['7:00:0', '10:00:00'])
    expected_error = ("['7:00:0', '10:00:00'] {}".format(msg))
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        utils_validation._check_time_range_format(['10:00:00'])
    expected_error = ("['10:00:00'] {}".format(msg))
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        utils_validation._check_time_range_format('10:00:00')
    expected_error = ("10:00:00 {}".format(msg))
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        utils_validation._check_time_range_format([100000, 170000])
    expected_error = ("[100000, 170000] {}".format(msg))
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        utils_validation._check_time_range_format(['10:00:00', '07:00:00'])
    expected_error = ("['10:00:00', '07:00:00'] {}".format(msg))
    assert expected_error in str(excinfo.value)
