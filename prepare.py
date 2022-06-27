import pandas as pd

def prepare_sales_data(df):
    ''' Prepares the sales data from the acquire.join_tables() function '''
    df.sale_date = pd.to_datetime(df.sale_date, format='%a, %d %b %Y %H:%M:%S %Z')
    df['month'] = df.sale_date.dt.month
    df['day of the week'] = df.sale_date.dt.weekday
    df['sales_total'] = df['sale_amount']*df['item_price']
    return df

def prepare_energy_german(df):
    ''' prepares the german energy data from acquire.get_german_energy_data() '''
    df.Date = pd.to_datetime(df.Date)
    df['month'] = df['Date'].dt.month
    df['year'] = df['Date'].dt.year
    df = df.fillna(0)
    return df