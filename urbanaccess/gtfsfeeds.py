import yaml
import pandas as pd
import urllib
from urllib2 import urlopen
import traceback
import zipfile
import os
import logging as lg
import time

from urbanaccess.utils import log
from urbanaccess import config


# TODO: make class CamelCase
class urbanaccess_gtfsfeeds(object):
    """
    A dict of GTFS feeds as {name of GTFS feed or transit service/agency :
    URL of feed} to request and
    download in the gtfs downloader.

    Parameters
    ----------
    gtfs_feeds : dict
        dictionary of the name of the transit service or agency GTFS feed
        as the key -note: this name will be used as the feed folder name.
        If the GTFS feed does not have a agency name in the agency.txt file
        this key will be used to name the agency- and
        the GTFS feed URL as the value to pass to the GTFS downloader as:
        {unique name of GTFS feed or transit service/agency : URL of feed}
    """

    def __init__(self,
                 gtfs_feeds={}):

        self.gtfs_feeds = gtfs_feeds

    @classmethod
    def from_yaml(cls, gtfsfeeddir=os.path.join(config.settings.data_folder,
                                                'gtfsfeeds'),
                  yamlname='gtfsfeeds.yaml'):
        """
        Create a urbanaccess_gtfsfeeds instance from a saved YAML.

        Parameters
        ----------
        gtfsfeeddir : str, optional
            Directory to load a YAML file.
        yamlname : str or file like, optional
            File name from which to load a YAML file.
        Returns
        -------
        urbanaccess_gtfsfeeds
        """

        if not isinstance(gtfsfeeddir, str):
            raise ValueError('gtfsfeeddir must be a string')
        if not os.path.exists(gtfsfeeddir):
            raise ValueError('{} does not exist or was not found'.format(
                gtfsfeeddir))
        if not isinstance(yamlname, str):
            raise ValueError('yaml must be a string')

        yaml_file = os.path.join(gtfsfeeddir, yamlname)

        with open(yaml_file, 'r') as f:
            yaml_config = yaml.load(f)

        if not isinstance(yaml_config, dict):
            raise ValueError('{} yamlname is not a dict'.format(yamlname))

        validkey = 'gtfs_feeds'
        if validkey not in yaml_config.keys():
            raise ValueError('key gtfs_feeds was not found in YAML file')

        for key in yaml_config['gtfs_feeds'].keys():
            if not isinstance(key, str):
                raise ValueError('{} must be a string'.format(key))
            for value in yaml_config['gtfs_feeds'][key]:
                if not isinstance(value, str):
                    raise ValueError('{} must be a string'.format(value))

        if (pd.Series(
                yaml_config['gtfs_feeds'].values()).value_counts() != 1).all():
            raise ValueError(
                'duplicate values were found when the passed add_dict '
                'dictionary was added to the existing dictionary. Feed URL '
                'values must be unique.')

        gtfsfeeds = cls(gtfs_feeds=yaml_config.get('gtfs_feeds', {}))
        log('{} YAML successfully loaded with {} feeds.'.format(yaml_file, len(
            yaml_config['gtfs_feeds'])))

        return gtfsfeeds

    def to_dict(self):
        """
        Return a dict representation of an urbanaccess_gtfsfeeds instance.
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
            If key of dict is already in the UrbanAccess replace
            the existing dict value with the value passed
        """

        if not isinstance(add_dict, dict):
            raise ValueError('add_dict is not a dict')
        if not isinstance(replace, bool):
            raise ValueError('replace is not bool')

        if replace is not True:

            for key in add_dict.keys():
                if key in self.gtfs_feeds.keys():
                    raise ValueError(
                        '{} passed in add_dict already exists in gtfs_feeds. '
                        'Only unique keys are allowed to be added.'.format(
                            key))
                if not isinstance(key, str):
                    raise ValueError('{} must be a string'.format(key))
                for value in add_dict[key]:
                    if not isinstance(value, str):
                        raise ValueError('{} must be a string'.format(value))

            for key, value in add_dict.items():
                if value in self.gtfs_feeds.values():
                    raise ValueError('duplicate values were found when the '
                                     'passed add_dict dictionary was added to '
                                     'the existing dictionary. Feed URL '
                                     'values must be unique.')
            gtfs_feeds = self.gtfs_feeds.update(add_dict)

        else:
            for key in add_dict.keys():
                if key in self.gtfs_feeds.keys():
                    log('{} passed in add_dict will replace existing {} feed '
                        'in gtfs_feeds.'.format(key, key))
                if not isinstance(key, str):
                    raise ValueError('{} must be a string'.format(key))
                for value in add_dict[key]:
                    if not isinstance(value, str):
                        raise ValueError('{} must be a string'.format(value))

            gtfs_feeds = self.gtfs_feeds.update(add_dict)

        log('Added {} feeds to gtfs_feeds: {}'.format(len(add_dict), add_dict))

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
                    raise ValueError(
                        '{} key to delete was not found in gtfs_feeds'.format(
                            key))
                del self.gtfs_feeds[key]
                log('Removed {} feed from gtfs_feeds'.format(key))

    def to_yaml(self, gtfsfeeddir=os.path.join(config.settings.data_folder,
                                               'gtfsfeeds'),
                yamlname='gtfsfeeds.yaml',
                overwrite=False):
        """
        Save a urbanaccess_gtfsfeeds representation to a YAML file.

        Parameters
        ----------
        gtfsfeeddir : str, optional
            Directory to save a YAML file.
        yamlname : str or file like, optional
            File name to which to save a YAML file.
        overwrite : bool, optional
            if true, overwrite an existing same name YAML file in specified
            directory
        Returns
        -------
        Nothing
        """

        if not isinstance(gtfsfeeddir, str):
            raise ValueError('gtfsfeeddir must be a string')
        if not os.path.exists(gtfsfeeddir):
            log(
                '{} does not exist or was not found and will be '
                'created'.format(
                    gtfsfeeddir))
            os.makedirs(gtfsfeeddir)
        if not isinstance(yamlname, str):
            raise ValueError('yaml must be a string')
        yaml_file = os.path.join(gtfsfeeddir, yamlname)
        if overwrite is False and os.path.isfile(yaml_file) is True:
            raise ValueError(
                '{} already exists. Rename or turn overwrite to True'.format(
                    yamlname))
        else:
            with open(yaml_file, 'w') as f:
                yaml.dump(self.to_dict(), f, default_flow_style=False)
            log('{} file successfully created'.format(yaml_file))


# instantiate the UrbanAccess gtfs feed object
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

    log(
        'Note: Your use of a GTFS feed is governed by each GTFS feed author '
        'license terms. It is suggested you read the respective license '
        'terms for the appropriate use of a GTFS feed.',
        level=lg.WARNING)

    if not isinstance(api, str):
        raise ValueError('{} must be a string'.format(api))
    if api not in config.settings.gtfs_api.keys():
        raise ValueError('{} is not currently a supported API'.format(api))
    if config.settings.gtfs_api[api] is None or not isinstance(
            config.settings.gtfs_api[api], str):
        raise ValueError('{} is not defined or defined incorrectly'.format(
            api))
    if not isinstance(match, str) or match not in ['contains', 'exact']:
        raise ValueError('match must be either: contains or exact')
    if not isinstance(add_feed, bool):
        raise ValueError('add_feed must be bool')

    if api == 'gtfsdataexch':
        log(
            'Warning: The GTFSDataExchange is no longer being maintained as '
            'of Summer 2016. '
            'Data accessed here may be out of date.', level=lg.WARNING)

        feed_table = pd.read_table(config.settings.gtfs_api[api], sep=',')
        feed_table['date_added'] = pd.to_datetime(feed_table['date_added'],
                                                  unit='s')
        feed_table['date_last_updated'] = pd.to_datetime(
            feed_table['date_last_updated'], unit='s')

        if search_text is None:
            log(
                'No search parameters were passed. Returning full list of {} '
                'GTFS feeds:'.format(
                    len(feed_table)))
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
                            feed_table[col].str.contains(text, case=False,
                                                         na=False)]
                    if match == 'exact':
                        search_result = feed_table[
                            feed_table[col].str.match(text, case=False,
                                                      na=False)]
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
                    log(
                        'Replaced all records in gtfs_feed list with the {} '
                        'found records:'.format(
                            len(search_result_df)))
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
             error_pause_duration=5, delete_zips=False):
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
        name of transit agency or service to use to name downloaded zipfile
    feed_url : str, optional
        corresponding URL to the feed_name to use to download GTFS feed zipfile
    feed_dict : dict, optional
        Dictionary specifying the name of the transit service or
        agency GTFS feed as the key and the GTFS feed URL as the value:
        {unique name of GTFS feed or transit service/agency : URL of feed}
    error_pause_duration : int, optional
        how long to pause in seconds before re-trying requests if error
    delete_zips : bool, optional
        if true the downloaded zipfiles will be removed
    Returns
    -------
    nothing
    """

    if (feed_name is not None and feed_url is None) or (
            feed_url is not None and feed_name is None):
        raise ValueError(
            'Both feed_name and feed_url parameters are required.')

    if feed_name is not None and feed_url is not None:
        if feed_dict is not None:
            raise ValueError('feed_dict is not specified')
        if not isinstance(feed_name, str) or not isinstance(feed_url, str):
            raise ValueError('either feed_name and or feed_url are not string')
        feeds.gtfs_feeds = {feed_name: feed_url}

    elif feed_dict is not None:
        if feed_name is not None or feed_url is not None:
            raise ValueError('either feed_name and or feed_url are not None')
        if not isinstance(feed_dict, dict):
            raise ValueError('feed_dict is not dict')
        for key in feed_dict.keys():
            if not isinstance(key, str):
                raise ValueError('{} must be a string'.format(key))
            for value in feed_dict[key]:
                if not isinstance(value, str):
                    raise ValueError('{} must be a string'.format(value))

        for key, value in feed_dict.items():
            if value in feed_dict.gtfs_feeds.values():
                raise ValueError(
                    'duplicate values were found when the passed add_dict '
                    'dictionary was added to the existing dictionary. Feed '
                    'URL values must be unique.')

        feeds.gtfs_feeds = feed_dict
    elif feed_name is None and feed_url is None and feed_dict is None:
        if len(feeds.gtfs_feeds) == 0:
            raise ValueError('No records were found in passed feed_dict')
        feeds.gtfs_feeds
    else:
        raise ValueError('Passed parameters were incorrect or not specified.')

    download_folder = os.path.join(data_folder, 'gtfsfeed_zips')

    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
        log('{} does not exist. Directory was created'.format(download_folder))
    log('{} GTFS feeds will be downloaded here: {}'.format(
        len(feeds.gtfs_feeds), download_folder))

    start_time1 = time.time()
    # TODO: add file counter and print number to user
    for feed_name_key, feed_url_value in feeds.gtfs_feeds.items():
        start_time2 = time.time()
        zipfile_path = ''.join([download_folder, '/', feed_name_key, '.zip'])

        if 'http' in feed_url_value:
            status_code = urllib.urlopen(feed_url_value).getcode()
            if status_code == 200:
                file = urlopen(feed_url_value)

                _zipfile_type_check(file=file,
                                    feed_url_value=feed_url_value)

                with open(zipfile_path, "wb") as local_file:
                    local_file.write(file.read())
                log(
                    '{} GTFS feed downloaded successfully. Took {:,'
                    '.2f} seconds for {:,.1f}KB'.format(
                        feed_name_key, time.time() - start_time2,
                        os.path.getsize(zipfile_path)))
            elif status_code in [429, 504]:
                log(
                    'URL at {} returned status code {} and no data. '
                    'Re-trying request in {:.2f} seconds.'.format(
                        feed_url_value, status_code, error_pause_duration),
                    level=lg.WARNING)
                time.sleep(error_pause_duration)
                try:
                    file = urlopen(feed_url_value)

                    _zipfile_type_check(file=file,
                                        feed_url_value=feed_url_value)

                    with open(zipfile_path, "wb") as local_file:
                        local_file.write(file.read())
                except Exception:
                    log('Unable to connect. URL at {} returned status code '
                        '{} and no data'.format(feed_url_value, status_code),
                        level=lg.ERROR)
            else:
                log(
                    'Unable to connect. URL at {} returned status code {} '
                    'and no data'.format(
                        feed_url_value, status_code), level=lg.ERROR)
        else:
            try:
                file = urlopen(feed_url_value)
                _zipfile_type_check(file=file,
                                    feed_url_value=feed_url_value)
                with open(
                        ''.join([download_folder, '/', feed_name_key, '.zip']),
                        "wb") as local_file:
                    local_file.write(file.read())
                log(
                    '{} GTFS feed downloaded successfully. Took {:,'
                    '.2f} seconds for {:,.1f}KB'.format(
                        feed_name_key, time.time() - start_time2,
                        os.path.getsize(zipfile_path)))
            except Exception:
                log('Unable to connect: {}'.format(traceback.format_exc()),
                    level=lg.ERROR)

    log('GTFS feed download completed. Took {:,.2f} seconds'.format(
        time.time() - start_time1))

    _unzip(zip_rootpath=download_folder, delete_zips=delete_zips)


def _unzip(zip_rootpath, delete_zips=True):
    """
    unzip all GTFS feed zipfiles in a root directory with resulting text files
    in the root folder: gtfsfeed_text

    Parameters
    ----------
    zip_rootpath : string
        root directory to place downloaded GTFS feed zipfiles
    delete_zips : bool, optional
        if true the downloaded zipfiles will be removed

    Returns
    -------
    nothing
    """

    start_time = time.time()

    unzip_rootpath = os.path.join(os.path.dirname(zip_rootpath),
                                  'gtfsfeed_text')

    if not os.path.exists(unzip_rootpath):
        os.makedirs(unzip_rootpath)
        log('{} does not exist. Directory was created'.format(unzip_rootpath))

    zipfilelist = [zipfilename for zipfilename in os.listdir(zip_rootpath) if
                   zipfilename.endswith(".zip")]
    if len(zipfilelist) == 0:
        raise ValueError('No zipfiles were found in specified '
                         'directory: {}'.format(zip_rootpath))

    for zfile in zipfilelist:

        with zipfile.ZipFile(os.path.join(zip_rootpath, zfile)) as z:
            # required to deal with zipfiles that have subdirectories and
            # that were created on OSX
            filelist = [file for file in z.namelist() if
                        file.endswith(".txt") and not file.startswith(
                            "__MACOSX")]
            if not os.path.exists(
                    os.path.join(unzip_rootpath, zfile.replace('.zip', ''))):
                os.makedirs(
                    os.path.join(unzip_rootpath, zfile.replace('.zip', '')))
            for file in filelist:
                with open(
                        os.path.join(unzip_rootpath, zfile.replace('.zip', ''),
                                     os.path.basename(file)), 'wb') as f:
                    f.write(z.read(file))
                    f.close()
            z.close()
            log('{} successfully extracted to: {}'.format(zfile, os.path.join(
                unzip_rootpath, zfile.replace('.zip', ''))))
    if delete_zips:
        os.remove(zip_rootpath)
        log('Deleted {} folder'.format(zip_rootpath))
    log(
        'GTFS feed zipfile extraction completed. Took {:,.2f} seconds for {} '
        'files'.format(
            time.time() - start_time, len(zipfilelist)))


def _zipfile_type_check(file, feed_url_value):
    """
    zipfile format checker helper

    Parameters
    ----------
    file : addinfourl
        loaded zipfile object in memory
    feed_url_value : str
        URL to download GTFS feed zipfile

    Returns
    -------
    nothing
    """
    if 'zip' not in file.info().dict['content-type'] \
            is True or 'octet' not in file.info().dict['content-type'] is True:
        raise ValueError(
            'data requested at {} is not a zipfile. '
            'Data must be a zipfile'.format(feed_url_value))
