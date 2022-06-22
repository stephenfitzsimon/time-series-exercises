import os
import pandas as pd

import requests

def get_api_df(domain, endpoint):
    '''gets a api call from domain+endpoint'''
    url = domain + endpoint
    endpoint_split = endpoint.split('/')
    name = endpoint_split[-1]
    response = requests.get(url)
    data = response.json()
    return pd.DataFrame(data['payload'][name])

def get_api_df_with_next_page(domain, endpoint, print_prog = False):
    '''gets an api call if there are multiple pages'''
    items = []
    endpoint_split = endpoint.split('/')
    name = endpoint_split[-1]
    i = 0
    while endpoint:
        url = domain + endpoint
        if print_prog:
            print(f"Getting info from {url}")
        response = requests.get(url)
        data = response.json()
        items.extend(data['payload'][name])
        # update the endpoint
        endpoint = data['payload']['next_page']
        i += 1
    return pd.DataFrame(items)

def get_api_table_data(table_name, query_api=False, print_prog=False):
    '''Acquires the api data from the database or the .csv file if if is present

    Args:
        query_api = False (Bool) :  Forces an api query and a resave of the data into a csv.
        print_prog = False (Bool) : prints which pages is currently being called
    Return:
        df (DataFrame) : a dataframe containing the data from the api call or the .csv file
    '''
    #set constants for the function
    filename = f'{table_name}.csv'
    endpoints = {
        'items':'/api/v1/items',
        'stores':'/api/v1/stores',
        'sales':'/api/v1/sales'
    }
    #file name string literal
    #check if file exists and query_dg flag
    if os.path.isfile(filename) and not query_api:
        #return dataframe from file
        print(f'Returning saved csv file : {filename}')
        return pd.read_csv(filename).drop(columns = ['Unnamed: 0'])
    else:
        #some constants
        domain = 'https://python.zgulde.net'
        endpoint = endpoints[table_name]
        #will store the api data
        items = []
        i = 0
        while endpoint:
            #make the url
            url = domain + endpoint
            #print progress if necessary
            if print_prog:
                print(f"Getting info from {url}")
            # api call
            response = requests.get(url)
            data = response.json()
            # add the new data
            items.extend(data['payload'][table_name])
            # update the endpoint
            endpoint = data['payload']['next_page']
            i += 1
        #make a dataframe, save it and return it
        df = pd.DataFrame(items)
        df.to_csv(filename)
        print(f'Saved as {filename}')
    return df

def get_all_tables(list_of_tables = ['items', 'stores', 'sales'], query_api_get_tables=False):
    '''Gets a data for all three tables'''
    #to store the data
    tables = dict()
    #api call for each table
    for t in list_of_tables:
        tables[t] = get_api_table_data(t, query_api=query_api_get_tables)
        print(f"Got table: {t}")
    return tables

def join_tables(query_api_join=False):
    '''Makes the overall dataframe 
    Args:
        query_api_join : forces an api query'''
    #get the tables
    tables_dict = get_all_tables(query_api_get_tables=query_api_join)
    #unpack the tables
    sales_df = tables_dict['sales']
    stores_df = tables_dict['stores']
    items_df = tables_dict['items']
    #join the tables on the keys
    join_table = tables_dict['sales'].merge(tables_dict['items'], left_on='item', right_on='item_id')
    join_table = join_table.merge(tables_dict['stores'], left_on='store', right_on='store_id')
    return join_table
    
def get_german_energy_data(url='https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'):
    df = pd.read_csv(url)
    return df