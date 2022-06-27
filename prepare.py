import pandas as pd
import acquire

def prep_HEB_data(df):
    ''''
    Takes in HEB store dateframe and cleans the data.
    Returns: Pandas Dataframe
    '''
    #Drop the time and timezone, Convert sale date to datetime then store as index
    df.sale_date = df.sale_date.apply(lambda date: date[:-13])
    df.sale_date = pd.to_datetime(df.sale_date,format="%a, %d %b %Y")
    df = df.set_index('sale_date').sort_index()
    #create month/day of week in format 01-Jan and 1-Mon. Autosorts columns in order and allows for more user-friendly string display
    df['month'] = df.index.strftime(date_format="%m-%b")
    df['dayofweek'] = df.index.strftime(date_format="%w-%a")
    #rename sale_amount
    df.rename(columns={'sale_amount':'quantity'},inplace=True)
    #Calculate sale_total
    df['sales_total'] = df.quantity * df.item_price
    return df

def wrangle_HEB_data():
    df = acquire.get_HEB_data()
    return prep_HEB_data(df)

def prep_OPSD_data(df):
    ''''
    Takes in OPS dateframe and cleans the data.
    Returns: Pandas Dataframe
    '''
    #Convert date to datetime and set as index
    df.Date = pd.to_datetime(df.Date,format="%Y-%m-%d")
    df = df.set_index('Date').sort_index()
    #Create month and year column
    df['month'] = df.index.strftime(date_format="%m-%b")
    df['year'] = df.index.year
    df['year'] = df['year'].astype(str)
    #Rename other columns
    df.rename(columns={"Wind+Solar":"wind_solar","Wind":"wind","Solar":"solar"},inplace=True)
    #fill nulls as zero
    df.fillna(0,inplace=True)
    #recalculate wind_solar
    df.wind_solar = df.wind + df.solar
    return df

def wrangle_OPSD_data():
    df = acquire.get_OPSD_data()
    return prep_OPSD_data(df)
    