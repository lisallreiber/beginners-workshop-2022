import dlt
import requests
# import os
# from notion-client import Client


@dlt.source
def notion_source(api_secret_key=dlt.secrets.value):
    return notion_resource(api_secret_key)


def _create_auth_headers(api_secret_key):
    """Constructs Bearer type authorization header which is the most common authorization method"""
    headers = {
        "Authorization": f"Bearer {api_secret_key}",
        "Notion-Version": "2021-05-11"
    }
    return headers


@dlt.resource(write_disposition="append")
def notion_resource(api_secret_key=dlt.secrets.value):
    headers = _create_auth_headers(api_secret_key)

    # notion = Client(auth=os.environ[api_secret_key])

    #from pprint import pprint
    #list_users_response = notion.users.list()
    #pprint(list_users_response)
    
    # check if authentication headers look fine
    # print(headers)
    
    PAGE_ID = "41ad748c62334885949a54a2d8ad8adc"
    url = f"https://api.notion.com/v1/pages/{PAGE_ID}"
    # params = {"query": }

    # make an api call here
    
    #response = requests.get(url, headers=headers, params=params)
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    yield response.json()

    # test data for loading validation, delete it once you yield actual data
    # test_data = [{'id': 0}, {'id': 1}]
    # yield test_data


if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='notion', destination='bigquery', dataset_name='notion_data')

    # print credentials by running the resource
    data = list(notion_resource())

    # print the data yielded from resource
    # print(data)
    import json
    print(json.dumps(data, indent=4))
    # exit()

    # run the pipeline with your parameters
    load_info = pipeline.run(notion_source())

    # pretty print the information on data that was loaded
    print(load_info)
