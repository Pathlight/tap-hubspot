ENDPOINTS_CONFIG = {
    'threads': {
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
                #assuming message conversations don't exceed the maximum limit of 500
                'paginate': False,
            }
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