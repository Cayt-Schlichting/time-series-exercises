import requests
import numpy as np
import pandas as pd
import os

def get_new_OPSD_data():
    df = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
    df.to_csv("opsd.csv",index=False)
    return df

def get_OPSD_data():
    """
    Retrieves OPSD dataset from working directory or jenfly github. Stores a local copy if one did not exist.
    Returns: Pandas dataframe of OPSD data
    """
    filename='opsd.csv'
    if os.path.isfile(filename): #check if file exists in WD
        #grab data, set first column as index
        return pd.read_csv(filename)
    else: #Get data from SQL db
        df = get_new_OPSD_data()
    return df

def get_new_HEB_data():
    #Set domain
    domain = 'https://python.zgulde.net/'
    ### GET SALES DATA
    endpoint = '/api/v1/sales'
    sales = []
    #loop while our endpoint isn't none
    while endpoint != None:
        #create api url
        url = domain + endpoint
        #get data
        response = requests.get(url)
        data = response.json()
        #add data to list
        sales.extend(data['payload']['sales'])
        #grab new endpoint
        endpoint = data['payload']['next_page']
    #save dataframe
    sales = pd.DataFrame(sales)

    ### GET STORE DATA
    endpoint = '/api/v1/stores'
    stores = []
    #loop while our endpoint isn't none
    while endpoint != None:
        #create api url
        url = domain + endpoint
        #get data
        response = requests.get(url)
        data = response.json()
        #add data to list
        stores.extend(data['payload']['stores'])
        #grab new endpoint
        endpoint = data['payload']['next_page']
    #save dataframe
    stores = pd.DataFrame(stores)

    ### GET ITEM DATA
    endpoint = '/api/v1/items'
    items = []
    #loop while our endpoint isn't none
    while endpoint != None:
        #create api url
        url = domain + endpoint
        #get data
        response = requests.get(url)
        data = response.json()
        #add data to list
        items.extend(data['payload']['items'])
        #grab new endpoint
        endpoint = data['payload']['next_page']
    items = pd.DataFrame(items)
    
    sales_items = sales.merge(items,left_on='item',right_on='item_id')
    combined_df = sales_items.merge(stores,left_on='store',right_on='store_id')
    #save it as a csv
    combined_df.to_csv("HEB_sales.csv",index=False)
    return combined_df


def get_HEB_data():
    """
    Retrieves HEB sales dataset from working directory or through API. Stores a local copy if one did not exist.
    Returns: Pandas dataframe of HEB sales data
    """
    filename='HEB_sales.csv'
    if os.path.isfile(filename): #check if file exists in WD
        #grab data, set first column as index
        return pd.read_csv(filename)
    else: #Get data from SQL db
        df = get_new_HEB_data()
    return df