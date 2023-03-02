from datetime import datetime, timedelta

import time
import singer
from singer import metrics, metadata, Transformer
from singer.bookmarks import set_currently_syncing

from tap_hubspot.discover import discover
from tap_hubspot.endpoints import ENDPOINTS_CONFIG

LOGGER = singer.get_logger()

ACTOR_LIST = set()

def write_schema(stream):
    schema = stream.schema.to_dict()
    singer.write_schema(stream.tap_stream_id, schema, stream.key_properties)

def isSyncActor(url_key, key_bag):
    #Don't make a duplicate call to fetch 'actor' information
    if url_key in key_bag:
        actor=key_bag['actor_id']
        if actor not in ACTOR_LIST:
            ACTOR_LIST.add(actor)
            return True
        else:
         return False

def update_key_bag_for_child(key_bag, parent_endpoint, record):
    # Constructs the properties needed to build the nested
    # paths used by the Hubspot APIs.
    # Ex. the list of messages is fetched at
    #   /threads/{threadId}/messages
    # We get the list of thread records and pass the following
    # key bag to the messages endpoint:
    #   {"threadId": <thread_id>}
    updated_key_bag = dict(key_bag)
  
    if parent_endpoint and 'provides' in parent_endpoint:
        for dest_key, obj_key in parent_endpoint['provides'].items():
        
            # Only sync children that have non-null values
            # Example: AssigneTo (actor_id) values can be blank for ceartain 'thread' records
            if obj_key in record:
                updated_key_bag[dest_key] = record[obj_key]
    
    return updated_key_bag

def sync_child_endpoints(client,
                         catalog,
                         state,
                         required_streams,
                         selected_streams,
                         stream_name,
                         endpoint,
                         key_bag,
                         records=[]):

    if 'children' not in endpoint or len(records) == 0:
        return

    for child_stream_name, child_endpoint in endpoint['children'].items():

        if child_stream_name not in required_streams:
            continue

        update_current_stream(state, child_stream_name)
        for record in records:
            # Iterate through records and fill in relevant keys
            # # for child streams.
            # # Ex. 'messages' requires a thread_Id in the path.
            child_key_bag = update_key_bag_for_child(key_bag, endpoint, record)
            
            # Only sync children that have non-null values
            # Example: AssigneTo (actor_id) values can be blank for ceartain 'thread' records
            if child_endpoint['url_key'] in child_key_bag:
                sync_endpoint(client,
                          catalog,
                          state,
                          required_streams,
                          selected_streams,
                          child_stream_name,
                          child_endpoint,
                          child_key_bag)
            
def sync_endpoint(client,
                  catalog,
                  state,
                  required_streams,
                  selected_streams,
                  stream_name,
                  endpoint,
                  key_bag,
                  stream_params={}):
    
    persist = endpoint.get('persist', True)

    if persist:
        stream = catalog.get_stream(stream_name)
        schema = stream.schema.to_dict()
        mdata = metadata.to_map(stream.metadata)
        write_schema(stream)

    path = endpoint['path'].format(**key_bag)
    
    #Don't make a duplicate call to fetch 'actor' information
    if stream_name=='actors' and not isSyncActor(endpoint['url_key'], key_bag):
        return
    
    # API Parameters
    # Maximum allowed limit is 500 
    # Reference https://developers.hubspot.com/docs/api/conversations/conversations
    limit = 500
    sort ='latestMessageTimestamp'
    after = ''
    start_date=''

    initial_load = True

    params={}

    if stream_name == 'threads':
        #ISO 8601 Datetime format
        iso_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        start_date = singer.get_bookmark(state,
                                     stream_name,
                                     client.start_date)

        if start_date:
            start_datetime = singer.utils.strptime_to_utc(start_date)
        else:
            # If no start_date or bookmark available, default to start_date of the config
            start_datetime = singer.utils.strptime_to_utc(client.start_date)
        
        dt_string = str(start_datetime)
        dt_object = datetime.fromisoformat(dt_string)
        start_datetime = dt_object.strftime(iso_format)

        params = {
            'limit': str(limit),
            'sort': sort,
            'latestMessageTimestampAfter': str(start_datetime)
        }
    
    if stream_name == 'messages':
        params = {
            'limit': str(limit)
        }

    #after is the next_page_token param value for Hubspot
    while initial_load or len(after) > 0:

        if initial_load:
            initial_load = False
        if state.get('currently_syncing') != stream_name:
            # We may have just been syncing a child stream.
            # Update the currently syncing stream if needed.
            update_current_stream(state, stream_name)

        data = client.get(path,
                          params=params,
                          endpoint=stream_name,
                        )

        if data is None:
            return

        if 'data_key' in endpoint:
            records = data[endpoint['data_key']]
        else:
            records = [data]

        #if no records are received
        if not records:
            return

        #parse records
        with metrics.record_counter(stream_name) as counter:
                with Transformer() as transformer:
                    for record in records:
                        if persist and stream_name in selected_streams:
                            record = {**record, **key_bag}
                            try:
                                record_typed = transformer.transform(record,
                                                                schema,
                                                                mdata)
                            except Exception as e:
                                LOGGER.info("HUBSPOT Sync Exception: %s....Record: %s", e, record)
                            singer.write_record(stream_name, record_typed)
                            
                            counter.increment()
        
        #get child records (if any)
        sync_child_endpoints(client,
                             catalog,
                             state,
                             required_streams,
                             selected_streams,
                             stream_name,
                             endpoint,
                             key_bag,
                             records=records)

        #if records retrieved are less than the max limit
        # 1. set bookmark
        # 2. next page will be blank
        if len(records) < limit and 'createdAt' in record:
            singer.write_bookmark(state, stream_name, 'endDate', records[len(records)-1]['createdAt'])
            return
    
        # for records that expect to be paged
        if endpoint.get('paginate', True):
            #get next_page_token
            after = data.get('paging', '').get('next', '').get('after', '')
            #set 'after' token when results have pages
            params = {
                        'limit': str(limit),
                        'sort': sort,
                        'latestMessageTimestampAfter': str(start_datetime),
                        'after': after
                    }
                    
def update_current_stream(state, stream_name=None):  
    set_currently_syncing(state, stream_name) 
    singer.write_state(state)

def get_required_streams(endpoints, selected_stream_names):
    required_streams = []
    for name, endpoint in endpoints.items():
        child_required_streams = None
        if 'children' in endpoint:
            child_required_streams = get_required_streams(endpoint['children'],
                                                          selected_stream_names)
        if name in selected_stream_names or child_required_streams:
            required_streams.append(name)
            if child_required_streams:
                required_streams += child_required_streams

    return required_streams

def sync(client, catalog, state):
    if not catalog:
        catalog = discover()
        selected_streams = catalog.streams
    else:
        selected_streams = catalog.get_selected_streams(state)

    selected_stream_names = []
    for selected_stream in selected_streams:
        selected_stream_names.append(selected_stream.tap_stream_id)

    required_streams = get_required_streams(ENDPOINTS_CONFIG, selected_stream_names)

    for stream_name, endpoint in ENDPOINTS_CONFIG.items():
        if stream_name in required_streams:
            update_current_stream(state, stream_name)
            sync_endpoint(client,        
                          catalog,
                          state,
                          required_streams,
                          selected_stream_names,
                          stream_name,
                          endpoint,
                          {})

    update_current_stream(state)
    
    return state
