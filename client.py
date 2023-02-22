from datetime import datetime, timedelta

import json
import backoff
import requests
import singer
from singer import metrics
from ratelimit import limits, sleep_and_retry, RateLimitException
from requests.exceptions import ConnectionError

LOGGER = singer.get_logger()

def log_backoff_attempt(details):
    LOGGER.info("Error detected communicating with Hubspot, triggering backoff: %d try",
                details.get("tries"))

class HubspotClient(object):
    BASE_URL = 'https://api.hubapi.com/'

    def __init__(self, config):
        self.config = config
        self.__access_token = config.get('access_token')
        self.__session = requests.Session()
        self.__client_id = None
        self.__client_secret = None
        self.__refresh_token = None
        self.__redirect_uri = None
        self.__code = None
        self.__expires_at = None
        self.start_date = config.get('start_date')

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__session.close()

    def get_new_access_token(self):
        # This is for the oauth server-to-server access type. From the docs:
        # To get a new access token, your app should call the /oauth/token endpoint again with the account_credentials grant.
        # https://legacydocs.hubspot.com/docs/methods/oauth2/get-access-and-refresh-tokens
        if self.__refresh_token is not None:
            return
        
        headers = {
            'Content-Type': "application/x-www-form-urlencoded;charset=utf-8",
            'authorization': 'Bearer {}'.format(self.__access_token)
        }
        
        params = {
            'grant_type': 'authorization_code',
            'client_id': self.__client_id,
            'client_secret': self.__client_secret,
            'redirect_uri': self.__redirect_uri,
            'code': self.__code
        }

        data = self.request(
            'POST',
            url='https://api.hubapi.com/oauth/v1/token',
            headers=headers,
            params=params)
        self.__access_token = data['access_token']
        self.__expires_at = datetime.utcnow() + \
            timedelta(seconds=data['expires_in'] - 10) # pad by 10 seconds for clock drift

        with open(self.__config_path) as file:
            config = json.load(file)
        config['access_token'] = data['access_token']
        with open(self.__config_path, 'w') as file:
            json.dump(config, file, indent=2)

    def refresh_access_token(self):

        # https://legacydocs.hubspot.com/docs/methods/oauth2/refresh-access-token
        headers = {
            'Content-Type': "application/x-www-form-urlencoded;charset=utf-8",
            'authorization': 'Bearer {}'.format(self.__access_token)
        }
        
        params = {
            'grant_type': 'refresh_token',
            'client_id': self.__client_id,
            'client_secret': self.__client_secret,
            'refresh_token': self.__refresh_token
        }

        data = self.request(
            'POST',
            url='https://api.hubapi.com/oauth/v1/token',
            headers=headers,
            params=params)

        self.__access_token = data['access_token']
        self.__refresh_token = data['refresh_token']

        self.__expires_at = datetime.utcnow() + \
            timedelta(seconds=data['expires_in'] - 10) # pad by 10 seconds for clock drift

        ## refresh_token changes every call to refresh
        with open(self.__config_path) as file:
            config = json.load(file)
        config['refresh_token'] = data['refresh_token']
        with open(self.__config_path, 'w') as file:
            json.dump(config, file, indent=2)

    def check_and_renew_access_token(self):
        
        expired_token = self.__expires_at is None or self.__expires_at <= datetime.utcnow()

        if self.__access_token is None:
            self.get_new_access_token()
        elif expired_token:
            self.refresh_access_token()

    @backoff.on_exception(backoff.expo,
                          (RateLimitException,ConnectionError),
                          max_tries=8,
                          on_backoff=log_backoff_attempt,
                          factor=3)
    @limits(calls=300, period=60)
    def request(self,
                method,
                path=None,
                url=None,
                **kwargs):
        
        """
        if url is None:
            self.check_and_renew_access_token()   
        """
    
        if url is None and path:
            url = '{}{}'.format(self.BASE_URL, path)

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None
        
        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        kwargs['headers']['Authorization'] = 'Bearer {}'.format(self.__access_token)

        with metrics.http_request_timer(endpoint) as timer:
            try:
                if 'params' not in kwargs:
                    response = self.__session.request(method, url, headers=kwargs['headers'])
                else:
                 response = self.__session.request(method, url, headers=kwargs['headers'], params=kwargs['params'])
            except Exception as e:
                LOGGER.info("HUBSPOT Client Exception:, %s", e)
            metrics_status_code = response.status_code

            timer.tags[metrics.Tag.http_status_code] = metrics_status_code

        response.raise_for_status()

        return response.json()

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)