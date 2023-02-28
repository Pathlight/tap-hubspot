# tap-hubspot

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from HubSpot's [REST API](http://developers.hubspot.com/docs/overview)
- Extracts the following resources from HubSpot
    - [Conversations, inbox and messages](https://developers.hubspot.com/docs/api/conversations/conversations)
   
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Configuration

This tap requires a `config.json` which specifies the access token to use and a cutoff date for syncing historical data.  

Clients should obtain an access token by creating a Hubspot [private app](https://developers.hubspot.com/docs/api/migrate-an-api-key-integration-to-a-private-app).

For example:

```
{
  "access_token": "token",
  "start_date": "2023-02-22T18:00:00Z"   
}
```

To run `tap-hubspot` with the configuration file, use this command:

```bash
› tap-hubspot -c my-config.json
```
