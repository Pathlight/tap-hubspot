#!/usr/bin/env python3

import sys
import json
import singer

from tap_hubspot.client import HubspotClient
from tap_hubspot.endpoints import ENDPOINTS_CONFIG
from tap_hubspot.discover import discover
from tap_hubspot.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [ 
    "base_url",
    "client_id",
    "client_secret",
    "access_token",
    "refresh_token",
    "start_date"
]

def do_discover(client):
    LOGGER.info('Testing authentication')
    try:
        client.get(ENDPOINTS_CONFIG.get('threads').get('path'))
    except:
        raise Exception('Error could not authenticate with Hubspot')

    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')

@singer.utils.handle_top_exception(LOGGER)
def main():
    
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    
    with HubspotClient(parsed_args.config, parsed_args.config_path) as client:
        if parsed_args.discover:
            do_discover(client)
        else:
            sync(client,
                 parsed_args.catalog,
                 parsed_args.state)
