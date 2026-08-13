import pandas as pd
import traceback
import zipfile
import os
import logging as lg
import time
import ssl
from six.moves.urllib import request
import shutil
from io import BytesIO

from urbanaccess.utils import log
from urbanaccess import config
from urbanaccess.utils import _dict_to_yaml, _yaml_to_dict


# TODO: make class CamelCase
class urbanaccess_gtfsfeeds(object):
    """
    A dict of GTFS feeds as {name of GTFS feed or transit service/agency :
    URL of feed} to request and
    download in the GTFS downloader.

    Parameters
    ----------
    gtfs_feeds : dict
        dictionary of the name of the transit service or agency GTFS feed
        as the key (Note: this name will be used as the feed folder name.
        If the GTFS feed does not have a agency name in the agency.txt file
        this key will be used to name the agency) and
        the GTFS feed URL as the value to pass to the GTFS downloader as:
        {unique name of GTFS feed or transit service/agency : URL of feed}
    """

    def __init__(self, gtfs_feeds={}):

        self.gtfs_feeds = gtfs_feeds

    @classmethod
    def from_yaml(cls,
                  gtfsfeeddir=os.path.join(
                      config.settings.data_folder, 'gtfsfeeds'),
                  yamlname='gtfsfeeds.yaml'):
        """
        Create an urbanaccess_gtfsfeeds instance from a saved YAML.

        Parameters
        ----------
        gtfsfeeddir : str, optional
            Directory to load a YAML file.
        yamlname : str or file like, optional
            File name from which to load a YAML file.

        Returns
        -------
        gtfsfeeds : object
        """
        yaml_config = _yaml_to_dict(yaml_dir=gtfsfeeddir, yaml_name=yamlname)

        validkey = 'gtfs_feeds'
        if validkey not in yaml_config.keys():
            raise ValueError('key gtfs_feeds was not found in YAML file')

        dtype_raise_error_msg = '{} must be a string.'
        for key in yaml_config['gtfs_feeds'].keys():
            if not isinstance(key, str):
                raise ValueError(dtype_raise_error_msg.format(key))
            value = yaml_config['gtfs_feeds'][key]
            if not isinstance(value, str):
                raise ValueError(dtype_raise_error_msg.format(value))
        unique_url_count = len(
            pd.DataFrame.from_dict(
                yaml_config['gtfs_feeds'], orient='index')[0].unique())
        url_count = len(yaml_config['gtfs_feeds'])
        if unique_url_count != url_count:
            raise ValueError(
                'duplicate values were found in YAML file: {}. Feed URL '
                'values must be unique.'.format(yamlname))

        gtfsfeeds = cls(gtfs_feeds=yaml_config.get('gtfs_feeds', {}))
        log('{} YAML successfully loaded with {:,} feeds.'.format(
            yamlname, len(yaml_config['gtfs_feeds'])))

        return gtfsfeeds

    def to_dict(self):
        """
        Return a dict representation of an urbanaccess_gtfsfeeds instance.

        Returns
        -------
        {'gtfs_feeds': urbanaccess_gtfsfeeds.gtfs_feeds} : dict
        """
        return {'gtfs_feeds': self.gtfs_feeds}

    def add_feed(self, add_dict, replace=False):
        """
        Add a dictionary to the urbanaccess_gtfsfeeds instance.

        Parameters
        ----------
        add_dict : dict
            Dictionary to add to existing urbanaccess_gtfsfeeds with the name
            of the transit service or agency GTFS feed as the key and the
            GTFS feed URL as the value to pass to the GTFS downloader
            as:
            {unique name of GTFS feed or transit service/agency : URL of feed}
        replace : bool, optional
            If key of dict is already in the urbanaccess_gtfsfeeds object,
            replace the existing dict value with the value passed

        Returns
        -------
        urbanaccess_gtfsfeeds : object
        """

        if not isinstance(add_dict, dict):
            raise ValueError('add_dict is not a dict')
        if not isinstance(replace, bool):
            raise ValueError('replace is not bool')
        dtype_raise_error_msg = '{} must be a string'

        # TODO: refactor to remove the need to have repeat code if
        #  replace True/False
        if replace is False:
            for key in add_dict.keys():
                if key in self.gtfs_feeds.keys():
                    msg = ('{} passed in add_dict already exists in '
                           'gtfs_feeds. Only unique keys are allowed to be '
                           'added.')
                    raise ValueError(msg.format(key))
                if not isinstance(key, str):
                    raise ValueError(dtype_raise_error_msg.format(key))
                value = add_dict[key]
                if not isinstance(value, str):
                    raise ValueError(dtype_raise_error_msg.format(value))

            for key, value in add_dict.items():
                if value in self.gtfs_feeds.values():
                    msg = ('duplicate values were found when the passed '
                           'add_dict dictionary was added to the existing '
                           'dictionary. Feed URL values must be unique.')
                    raise ValueError(msg)
            gtfs_feeds = self.gtfs_feeds.update(add_dict)

        else:
            for key in add_dict.keys():
                if key in self.gtfs_feeds.keys():
                    log('{} passed in add_dict will replace existing {} feed '
                        'in gtfs_feeds.'.format(key, key))
                if not isinstance(key, str):
                    raise ValueError(dtype_raise_error_msg.format(key))
                value = add_dict[key]
                if not isinstance(value, str):
                    raise ValueError(dtype_raise_error_msg.format(value))
            gtfs_feeds = self.gtfs_feeds.update(add_dict)

        log('Added {:,} feeds to gtfs_feeds: {}'.format(
            len(add_dict), add_dict))

        return gtfs_feeds

    def remove_feed(self, del_key=None, remove_all=False):
        """
        Remove GTFS feeds from the existing urbanaccess_gtfsfeeds instance

        Parameters
        ----------
        del_key : str or list, optional
            dict keys as a single string or list of
            strings to remove from existing
        remove_all : bool, optional
            if true, remove all keys from existing
            urbanaccess_gtfsfeeds instance

        Returns
        -------
        Nothing

        """
        if not isinstance(remove_all, bool):
            raise ValueError('remove_all is not bool')

        if del_key is None and remove_all:
            self.gtfs_feeds = {}
            log('Removed all feeds from gtfs_feeds')
        else:
            if not isinstance(del_key, (list, str)):
                raise ValueError('del_key must be a string or list of strings')
            if remove_all:
                raise ValueError(
                    'remove_all must be False in order to remove individual '
                    'records: {}'.format(del_key))

            del_key = [del_key]

            for key in del_key:
                if key not in self.gtfs_feeds.keys():
                    msg = '{} key to delete was not found in gtfs_feeds'
                    raise ValueError(msg.format(key))
                del self.gtfs_feeds[key]
                log('Removed {} feed from gtfs_feeds'.format(key))

    def to_yaml(self, gtfsfeeddir=os.path.join(
        config.settings.data_folder, 'gtfsfeeds'),
                yamlname='gtfsfeeds.yaml', overwrite=False):
        """
        Save an urbanaccess_gtfsfeeds representation to a YAML file.

        Parameters
        ----------
        gtfsfeeddir : str, optional
            Directory to save a YAML file.
        yamlname : str or file like, optional
            File name to which to save a YAML file.
        overwrite : bool, optional
            if true, will overwrite an existing YAML
            file in specified directory if file names are the same
        Returns
        -------
        Nothing
        """
        _dict_to_yaml(dictionary=self.to_dict(), yaml_dir=gtfsfeeddir,
                      yaml_name=yamlname, overwrite=overwrite)


# instantiate the UrbanAccess GTFS feed object
feeds = urbanaccess_gtfsfeeds()


def search(api='gtfsdataexch', search_text=None, search_field=None,
           match='contains', add_feed=False, overwrite_feed=False):
    """
    Connect to a GTFS feed repository API and search for GTFS feeds that exist
    in a remote GTFS repository and whether or not to add the GTFS feed name
    and download URL to the urbanaccess_gtfsfeeds instance.
    Currently only supports access to the GTFS Data Exchange API.

    Parameters
    ----------
    api : {'gtfsdataexch'}, optional
        name of GTFS feed repository to search in. name corresponds to the
        dict specified in the urbanacess_config instance. Currently only
        supports access to the GTFS Data Exchange repository.
    search_text : str, optional
        string pattern to search for
    search_field : string or list, optional
        name of the field or column to search for string
    match : {'contains', 'exact'}, optional
        search string matching method as either: contains or exact
    add_feed : bool, optional
        add search results to existing urbanaccess_gtfsfeeds instance using
        the name field as the key and the URL as the value
    overwrite_feed : bool, optional
        If true the existing urbanaccess_gtfsfeeds instance will be replaced
        with the records returned in the search results.
        All existing records will be removed.
    Returns
    -------
    search_result_df : pandas.DataFrame
        Dataframe of search results displaying full feed metadata
    """
    msg = ('Note: Your use of a GTFS feed is governed by each GTFS feed author'
           ' license terms. It is suggested you read the respective license '
           'terms for the appropriate use of a GTFS feed.')
    log(msg, level=lg.WARNING)

    if not isinstance(api, str):
        raise ValueError('{} must be a string'.format(api))
    if api not in config.settings.gtfs_api.keys():
        raise ValueError('{} is not currently a supported API'.format(api))
    if config.settings.gtfs_api[api] is None or not isinstance(
            config.settings.gtfs_api[api], str):
        raise ValueError('{} API is not defined or is '
                         'defined incorrectly'.format(api))
    if not isinstance(match, str) or match not in ['contains', 'exact']:
        raise ValueError('match must be either: contains or exact')
    if not isinstance(add_feed, bool):
        raise ValueError('add_feed must be bool')

    if api == 'gtfsdataexch':
        log('Warning: The GTFSDataExchange is no longer being maintained as '
            'of Summer 2016. Data accessed here may be out of date.',
            level=lg.WARNING)

        feed_table = pd.read_table(config.settings.gtfs_api[api], sep=',')
        feed_table['date_added'] = pd.to_datetime(
            feed_table['date_added'], unit='s')
        feed_table['date_last_updated'] = pd.to_datetime(
            feed_table['date_last_updated'], unit='s')

        if search_text is None:
            log('No search parameters were passed. Returning full list of {} '
                'GTFS feeds:'.format(len(feed_table)))
            return feed_table
        else:
            pass

        search_result_df = pd.DataFrame()

        if search_field is None:
            search_field = ['name', 'url', 'dataexchange_id', 'feed_baseurl']
        else:
            if not isinstance(search_field, list):
                raise ValueError('search_field is not list')

        for field in search_field:
            if field not in feed_table.columns:
                raise ValueError(
                    '{} column not found in available feed table'.format(
                        field))
            for col in feed_table.select_dtypes(include=[object]).columns:

                if isinstance(search_text, str):
                    search_text = [search_text]
                else:
                    if not isinstance(search_text, list):
                        raise ValueError('search_text is not list')

                for text in search_text:
                    if match == 'contains':
                        search_result = feed_table[
                            feed_table[col].str.contains(
                                text, case=False, na=False)]
                    if match == 'exact':
                        search_result = feed_table[
                            feed_table[col].str.match(
                                text, case=False, na=False)]
                    search_result_df = search_result_df.append(search_result)
                    search_result_df.drop_duplicates(inplace=True)

        log('Found {} records that matched {} inside {} columns:'.format(
            len(search_result_df), search_text, search_field))

        if len(search_result_df) != 0:
            if add_feed:
                if overwrite_feed:
                    zip_url = search_result_df[
                                  'dataexchange_url'] + 'latest.zip'
                    search_result_df['dataexchange_url'] = zip_url
                    search_result_dict = search_result_df.set_index('name')[
                        'dataexchange_url'].to_dict()
                    feeds.gtfs_feeds = search_result_dict
                    log('Replaced all records in gtfs_feed list with the {} '
                        'found records:'.format(len(search_result_df)))
                else:
                    zip_url = search_result_df[
                                  'dataexchange_url'] + 'latest.zip'
                    search_result_df['dataexchange_url'] = zip_url
                    search_result_dict = search_result_df.set_index('name')[
                        'dataexchange_url'].to_dict()
                    feeds.add_feed(search_result_dict)
                    log('Added {} records to gtfs_feed list:'.format(
                        len(search_result_df)))
                return search_result_dict
            else:
                return search_result_df


def download(data_folder=os.path.join(config.settings.data_folder),
             feed_name=None, feed_url=None, feed_dict=None,
             error_pause_duration=5, delete_zips=False, raise_url_error=True):
    """
    Connect to the URLs passed in function or the URLs stored in the
    urbanaccess_gtfsfeeds instance and download the GTFS feed zipfile(s)
    then unzip inside a local root directory. Resulting GTFS feed text files
    will be located in the root folder: gtfsfeed_text unless otherwise
    specified

    Parameters
    ----------
    data_folder : str, optional
        directory to download GTFS feed data to
    feed_name : str, optional
        name of transit agency or service to use to name downloaded zipfile.
        If using feed_name and feed_url, cannot use feed_dict.
    feed_url : str, optional
        corresponding URL to the feed_name to use to download GTFS feed
        zipfile. If using feed_name and feed_url, cannot use feed_dict.
    feed_dict : dict, optional
        Dictionary specifying the name of the transit service or
        agency GTFS feed as the key and the GTFS feed URL as the value:
        {unique name of GTFS feed or transit service/agency : URL of feed}.
        If using feed_dict, cannot use feed_name and feed_url.
    error_pause_duration : int, optional
        how long to pause in seconds before re-trying requests if error
    delete_zips : bool, optional
        if true the directory created by this function called 'gtfsfeed_zips'
        that holds the downloaded zipfiles from each URL will be deleted.
        This directory is located inside of the root data directory
        specified in the 'data_folder' parameter under the the GTFS feed
        directory for that zipfile for example:
        data_folder\\feed_folder\\gtfsfeed_zips, default is False
    raise_url_error : bool, optional
        if true error will be raised when a request to a URL fails, URL
        failure response will be returned, else if false URL failures will
        still be printed to logs but will fail silently to allow process
        to continue.
    Returns
    -------
    Nothing
    """
    dtype_raise_error_msg = '{} must be a string'
    if (feed_name is not None and feed_url is None) or (
            feed_url is not None and feed_name is None):
        raise ValueError(
            'Both feed_name and feed_url parameters are required.')

    if feed_name is not None and feed_url is not None:
        if feed_dict is not None:
            raise ValueError('only feed_dict or feed_name and '
                             'feed_url can be used at once. '
                             'Both cannot be used.')
        if not isinstance(feed_name, str) or not isinstance(feed_url, str):
            raise ValueError('either feed_name and or feed_url are not string')
        feeds.gtfs_feeds = {feed_name: feed_url}

    elif feed_dict is not None:
        if not isinstance(feed_dict, dict):
            raise ValueError('feed_dict is not dict')
        for key in feed_dict.keys():
            if not isinstance(key, str):
                raise ValueError(dtype_raise_error_msg.format(key))
            value = feed_dict[key]
            if not isinstance(value, str):
                raise ValueError(dtype_raise_error_msg.format(value))

        feed_dict_urls = list(feed_dict.values())
        has_dup_urls = len(set(feed_dict_urls)) != len(feed_dict_urls)
        if has_dup_urls:
            raise ValueError('duplicate values were found in feed_dict. '
                             'Feed URL values must be unique.')

        feeds.gtfs_feeds = feed_dict
    elif feed_name is None and feed_url is None and feed_dict is None:
        if len(feeds.gtfs_feeds) == 0:
            raise ValueError('No records were found in passed feed_dict')

    download_folder = os.path.join(data_folder, 'gtfsfeed_zips')

    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
        log('{} does not exist. Directory was created'.format(download_folder))
    log('{:,} GTFS feed(s) will be downloaded here: {}'.format(
        len(feeds.gtfs_feeds), download_folder))

    start_time1 = time.time()
    msg_no_connection_w_status = ('Unable to connect. URL at {} returned '
                                  'status code {} and no data')
    msg_no_connection = 'Unable to connect to: {}. Error: {}'
    msg_download_succeed = ('{} GTFS feed downloaded successfully. '
                            'Took {:,.2f} seconds for {:,.1f}KB')
    # TODO: add file counter and print number to user
    for feed_name_key, feed_url_value in feeds.gtfs_feeds.items():
        start_time2 = time.time()
        zipfile_name = '{}.zip'.format(feed_name_key)
        zipfile_path = os.path.join(download_folder, zipfile_name)

        # resolve issues where request results in certificate verify failure
        ssl._create_default_https_context = ssl._create_unverified_context

        # add default user-agent header in request to avoid 403 Errors
        opener = request.build_opener()
        opener.addheaders = [('User-agent', '')]
        request.install_opener(opener)

        if 'http' in feed_url_value:
            try:
                status_code = request.urlopen(feed_url_value).getcode()
                if status_code == 200:
                    file = request.urlopen(feed_url_value)

                    _zipfile_type_check(file=file,
                                        feed_url_value=feed_url_value)

                    with open(zipfile_path, "wb") as local_file:
                        local_file.write(file.read())
                    log(msg_download_succeed.format(
                        feed_name_key, time.time() - start_time2,
                        os.path.getsize(zipfile_path)))
                # deal with 429 Too Many Requests and 504 Gateway Timeout
                # separately
                elif status_code in [429, 504]:
                    msg = ('URL at {} returned status code {} and no data. '
                           'Re-trying request in {:.2f} seconds.')
                    log(msg.format(feed_url_value, status_code,
                                   error_pause_duration),
                        level=lg.WARNING)
                    time.sleep(error_pause_duration)
                    try:
                        file = request.urlopen(feed_url_value)
                        _zipfile_type_check(
                            file=file, feed_url_value=feed_url_value)
                        with open(zipfile_path, "wb") as local_file:
                            local_file.write(file.read())
                    except Exception:
                        log(msg_no_connection_w_status.format(
                            feed_url_value, status_code),
                            level=lg.ERROR)
                        if raise_url_error:
                            raise Exception(
                                msg_no_connection_w_status.format(
                                    feed_url_value, status_code))
                else:
                    log(msg_no_connection_w_status.format(
                        feed_url_value, status_code),
                        level=lg.ERROR)
            except Exception:
                log(msg_no_connection.format(
                    feed_url_value, traceback.format_exc()),
                    level=lg.ERROR)
                if raise_url_error:
                    raise Exception(
                        msg_no_connection.format(
                            feed_url_value, traceback.format_exc()))
        else:
            # for non http links such as FTP
            try:
                file = request.urlopen(feed_url_value)
                _zipfile_type_check(file=file,
                                    feed_url_value=feed_url_value)
                with open(zipfile_path, "wb") as local_file:
                    local_file.write(file.read())
                log(msg_download_succeed.format(
                    feed_name_key, time.time() - start_time2,
                    os.path.getsize(zipfile_path)))
            except Exception:
                log(msg_no_connection.format(
                    feed_url_value, traceback.format_exc()),
                    level=lg.ERROR)
                if raise_url_error:
                    raise Exception(
                        msg_no_connection.format(
                            feed_url_value, traceback.format_exc()))

    log('GTFS feed download completed. Took {:,.2f} seconds'.format(
        time.time() - start_time1))

    unzip(zip_rootpath=download_folder, delete_zips=delete_zips)


def _list_zip_files_in_zip(zip_file_list):
    sub_zip_filelist = [file for file in zip_file_list if
                        file.endswith(".zip")]
    return sub_zip_filelist


def _list_txt_files_in_zip(zip_file_list):
    filelist = [file for file in zip_file_list if
                file.endswith(".txt") and not file.startswith("__MACOSX")]
    return filelist


def _validate_gtfs_zip_structure(zip_object):
    z_files = zip_object.namelist()
    # required to deal with zipfiles that have subdirectories and
    # that were created on OSX
    txt_filelist = _list_txt_files_in_zip(zip_file_list=z_files)
    # in cases where the zip contains multiple zips of GTFS feeds
    # such as the case with the SEPTA and Victoria Australia GTFS feeds
    sub_zip_filelist = _list_zip_files_in_zip(zip_file_list=z_files)
    sub_zip_cnt = len(sub_zip_filelist)
    # Note: there must only be zips inside of the sub directory, if its a
    # mix of zips and txt files we assume that the parent zip contained
    # both its zipped txt contents and a copy of itself as a zip in which
    # case the GTFS txt files will be dealt with correctly but we
    # will ignore the subdirectory zip
    if txt_filelist and sub_zip_filelist:
        msg = ('Warning: Zipfile contains {:,} zipfile(s): {} in addition '
               'to GTFS txt files: {}. Zipfile(s) inside of the parent '
               'zipfile will not be extracted.')
        log(msg.format(sub_zip_cnt, sub_zip_filelist, txt_filelist),
            level=lg.WARNING)
        sub_zip_filelist = []  # null subdirectory zipfile list

    sub_zip_cnt = len(sub_zip_filelist)
    if len(txt_filelist) == 0 and sub_zip_cnt > 0:
        msg = ('Zipfile contains {:,} zipfiles: {}. These will be '
               'extracted as separate feeds.')
        log(msg.format(sub_zip_cnt, sub_zip_filelist))
    return txt_filelist, sub_zip_filelist


def _unzip_util(zipfile_read_path, unzip_file_path):
    """
    unzip GTFS feed zipfile in a root directory with resulting text files
    in the root folder: gtfsfeed_text. If zipfile contains only zipfiles in
    its subdirectory, each zipfile will be extracted separately
    into their own directories inside of root folder: gtfsfeed_text

    Parameters
    ----------
    zipfile_read_path : string
        full path to directory where zipfile to unzip is located
    unzip_file_path : string
        full path to directory where to unzip zipfile

    Returns
    -------
    Nothing
    """
    with zipfile.ZipFile(zipfile_read_path, 'r') as z:
        txt_filelist, sub_zip_filelist = _validate_gtfs_zip_structure(
            zip_object=z)
        if not sub_zip_filelist:
            if not os.path.exists(unzip_file_path):
                log('{} does not exist. Directory was created'.format(
                    unzip_file_path))
                os.makedirs(unzip_file_path)
            for txt_file in txt_filelist:
                file_path = os.path.join(
                    unzip_file_path, os.path.basename(txt_file))
                with open(file_path, 'wb') as f:
                    f.write(z.read(txt_file))
                    f.close()
        else:
            for sub_zip_path in sub_zip_filelist:
                # read the inner zip
                sub_zip_z = z.read(sub_zip_path)
                with zipfile.ZipFile(BytesIO(sub_zip_z), 'r') as z_sub:
                    txt_filelist, sub_zip_filelist = \
                        _validate_gtfs_zip_structure(zip_object=z_sub)

                    # make unzip dir name unique in case others exist, there 
                    # can be cases where the sub_zip_path is located in a 
                    # nested directory so we use the subdirectories as part 
                    # of the new dir renaming schema and flatten it out to 
                    # just one directory per sub_zip_path
                    sub_zip_path_parts = os.path.split(sub_zip_path)
                    sub_zip_path_flat = '_'.join(
                        sub_zip_path_parts).replace('.zip', '')
                    sub_unzip_file_path = os.path.join(
                        os.path.dirname(unzip_file_path), sub_zip_path_flat)
                    
                    if not os.path.exists(sub_unzip_file_path):
                        log('{} does not exist. Directory was created'.format(
                                sub_unzip_file_path))
                        os.makedirs(sub_unzip_file_path)
                    for txt_file in txt_filelist:
                        file_path = os.path.join(
                            sub_unzip_file_path, os.path.basename(txt_file))
                        with open(file_path, 'wb') as f:
                            f.write(z_sub.read(txt_file))
                            f.close()
                    z_sub.close()
        z.close()


def unzip(zip_rootpath, delete_zips=False):
    """
    unzip all GTFS feed zipfiles in a root directory with resulting text files
    in the root folder: gtfsfeed_text

    Parameters
    ----------
    zip_rootpath : string
        root directory to place downloaded GTFS feed zipfiles
    delete_zips : bool, optional
        if true the directory specified in zip_rootpath will be deleted which
         will delete all the downloaded zipfiles, default is False

    Returns
    -------
    Nothing
    """
    start_time = time.time()

    unzip_rootpath = os.path.join(
        os.path.dirname(zip_rootpath), 'gtfsfeed_text')

    zipfilelist = [zipfilename for zipfilename in os.listdir(zip_rootpath) if
                   zipfilename.endswith(".zip")]
    if len(zipfilelist) == 0:
        raise ValueError('No zipfiles were found in specified '
                         'directory: {}'.format(zip_rootpath))

    for zfile in zipfilelist:
        unzipfile_name = zfile.replace('.zip', '')
        unzip_file_path = os.path.join(unzip_rootpath, unzipfile_name)
        zipfile_read_path = os.path.join(zip_rootpath, zfile)
        _unzip_util(
            zipfile_read_path=zipfile_read_path,
            unzip_file_path=unzip_file_path)
        log('{} successfully extracted to: {}'.format(
            zfile, unzip_file_path))

    if delete_zips:
        shutil.rmtree(zip_rootpath)
        log('Deleted {} folder and all its contents.'.format(zip_rootpath))
    msg = ('GTFS feed zipfile extraction completed. Took {:,.2f} seconds '
           'for {:,} file(s)')
    log(msg.format(time.time() - start_time, len(zipfilelist)))


def _zipfile_type_check(file, feed_url_value):
    """
    Helper function to check if HTTP response contains a zipfile.
    Valid application content are: 'zip' or 'octet-stream'

    Parameters
    ----------
    file : http.client.HTTPResponse
        loaded zipfile HTTPResponse object in memory
    feed_url_value : str
        URL to download GTFS feed zipfile for informative purposes

    Returns
    -------
    Nothing
    """
    # TODO: check for a better method to support cases that are zips but
    #  dont have header info example: sanfordcommunityredevelopmentagency
    #  GTFS feed URL
    content_type = file.info().get('Content-Type')
    if 'zip' not in content_type is True \
            or 'octet' not in content_type is True:
        raise ValueError(
            'data requested at {} is not a zipfile. '
            'Data must be a zipfile.'.format(feed_url_value))
