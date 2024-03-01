ENDPOINTS_CONFIG = {
    'threads': { # This API is in beta https://developers.hubspot.com/docs/api/conversations/conversations#threads-messages
        'persist': True,
        'path': 'conversations/v3/conversations/threads',
        'pk': ['id'],
        'data_key': 'results',
        'paginate': True,
        'provides': {
            'thread_id': 'id'
        },
        'children': {
            'messages': {
                'path': 'conversations/v3/conversations/threads/{thread_id}/messages',
                'pk': ['id'],
                'data_key': 'results',
                'url_key': 'thread_id',
                #assuming message conversations don't exceed the maximum limit of 500
                'paginate': False,
            } 
        }
    },
    'actors': {
        'persist': True,
        'path': 'conversations/v3/conversations/actors/batch/read',
        'data_key': 'results',
        'pk': ['id'],
        'paginate': False,
        'provides': {
            'actor_id': 'id'
        }
    },
    'inboxes': {
        'persist': True,
        'path': 'conversations/v3/conversations/inboxes',
        'pk': ['id'],
        'data_key': 'results',
        'paginate': False,
        'provides': {
            'inbox_id': 'id'
        }
    }
}

