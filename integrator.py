import pandas as pd
from urllib import request
import json
import xml.etree.cElementTree as et
import reverse_geocoder as rg
from datetime import datetime
from decimal import *


def convert_timestamp(row):
    '''
    takes in a transaction dataframe row and returns a timestamp
      in the format YYYY-MM-DD HH:mm:ss
    '''
    timestamp = row['timestamp']
    timestamp = datetime.fromtimestamp(timestamp / 1e3)
    timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return timestamp


def convert_amount(row):
    '''
    return a Decimal to 2 decimal places
    '''
    amount = row['amount']
    amount = Decimal(amount).quantize(Decimal('.01'))
    return amount


def get_city(row):
    '''
    row is a transaction row from a dataframe
    returns a city name
    '''
    coordinates = (row['latitude'], row['longitude'])
    results = rg.search(coordinates)
    return results[0].get('name')


def get_transactions():
    '''
    retrieves transactions from the url and returns a list containing
    transaction dictionary objects e.g
    [{"customerId": 3, "timestamp": 1539767520453, "amount": 5612.32,
        "latitude": -1.970579, "longitude": 30.104429}]
    '''
    url_str = 'https://df-alpha.bk.rw/interview01/transactions'
    with request.urlopen(url_str) as url:
        transactions = json.loads(url.read().decode())
    return transactions


def get_customers():
    '''
    retrieves customers from the url and returns a list containing
    customer dictionary objects e.g [{'name': 'Alice', 'id': 1}]
    '''
    url_str = 'https://df-alpha.bk.rw/interview01/customers'
    with request.urlopen(url_str) as url:
        customers = url.read()

    parsedXML = et.fromstring(customers)
    customers = []
    for node in parsedXML:
        name = node.find('name').text
        id = node.find('id').text
        customers.append({'id': int(id), 'name': name})

    return customers

    # data = [{"customerId": 3, "timestamp": 1539767520453, "amount": 5612.32, "latitude": -1.970579, "longitude": 30.104429}, {"customerId": 1, "timestamp": 1539767520453, "amount": 5612.32, "latitude": -1.970579, "longitude": 30.104429}, {"customerId": 1, "timestamp": 1539767722039, "amount": 2001, "latitude": -1.970579, "longitude": 30.104429}, {"customerId": 1, "timestamp": 1539767723735, "amount": 1987.11, "latitude": -1.970579, "longitude": 30.104429}, {"customerId": 1, "timestamp": 1539767724559, "amount": 9888.99, "latitude": -1.970579, "longitude": 30.104429}, {"customerId": 2, "timestamp": 1539767829151, "amount": 324234.99, "latitude": -1.292066, "longitude": 36.821945}, {"customerId": 2, "timestamp": 1539767830247, "amount": 12224.99, "latitude": -1.292066, "longitude": 36.821945}, {"customerId": 2, "timestamp": 1539767830951, "amount": 99221.22, "latitude": -1.292066, "longitude": 36.821945},
    #         {"customerId": 1, "timestamp": 1539767830951, "amount": 99221.22, "latitude": -1.292066, "longitude": 36.821945}, {"customerId": 2, "timestamp": 1539767830951, "amount": 99221.22, "latitude": -1.292066, "longitude": 36.821945}, {"customerId": 2, "timestamp": 1539767830951, "amount": 99221.22, "latitude": -1.292066, "longitude": 36.821945}, {"customerId": 3, "timestamp": 1539767905080, "amount": 1221.11, "latitude": -26.204103, "longitude": 28.047304}, {"customerId": 3, "timestamp": 1539767905801, "amount": 98711, "latitude": -26.204103, "longitude": 28.047304}, {"customerId": 3, "timestamp": 1539767906176, "amount": 2100119992, "latitude": -26.204103, "longitude": 28.047304}, {"customerId": 3, "timestamp": 1539767724559, "amount": 2222113.99, "latitude": -26.204103, "longitude": 28.047304}, {"customerId": 3, "timestamp": 1539767722039, "amount": 123.45, "latitude": -26.204103, "longitude": 28.047304}]


def get_transactions_df():
    '''
    return a dataframe of transactions
    '''
    transactions = get_transactions()
    df_transactions = pd.DataFrame.from_dict(transactions, orient='columns')
    return df_transactions


def get_customers_df():
    '''
    return a dataframe of customers
    '''
    customers = get_customers()
    df_customers = pd.DataFrame.from_dict(customers, orient='columns')
    return df_customers


def get_merged_dataframe(df_transactions, df_customers):
    '''
     merge customer and transaction dataframes so that all entries are on one line
    '''
    merged = pd.merge(
        df_transactions,
        df_customers,
        left_on='customerId',
        right_on='id',
        how='left'
    )

    print(merged)

    # add reverse geocoded city
    merged['city'] = merged.apply(get_city, axis=1)

    # format date
    merged['correct_timestamp'] = merged.apply(convert_timestamp, axis=1)

    # format amount
    merged['correct_amount'] = merged.apply(convert_amount, axis=1)

    # print final df
    print(merged.head())

    return merged


def get_cleaned_transactions(merged):
    df_transactions = merged[[
        'correct_timestamp',
        'customerId',
        'name',
        'correct_amount',
        'city'
    ]].copy(deep=True)

    df_transactions.rename(columns={
        'correct_timestamp': 'DateTime',
        'customerId': 'Customer_Id',
        'name': 'Customer_Name',
        'correct_amount': 'Amount',
        'city': 'City_Name'
    }, inplace=True)

    return df_transactions


def get_cleaned_customers(merged):
    df_customers = merged[['city', 'amount', 'name']].copy(deep=True)

    grouped_customers = df_customers.groupby(['city']).agg(
        {
            'city': 'count',
            'amount': 'sum',
            'name': pd.Series.nunique
        }
    )

    # rename columns
    grouped_customers.index.names = ['City_Name']
    grouped_customers.rename(columns={
        'amount': 'Total_Amount',
        'name': 'Unique_Customers',
        'city': 'Total_Transactions',
    }, inplace=True)

    # print(grouped_customers)

    return grouped_customers


if __name__ == "__main__":

    df_transactions = get_transactions_df()
    df_customers = get_customers_df()

    # print(df_transactions.head())
    # print(df_customers.head())

    merged = get_merged_dataframe(
        df_transactions,
        df_customers
    )
    df_transactions = get_cleaned_transactions(merged)
    df_customers = get_cleaned_customers(merged)

    # print(df_transactions.head())
    # print(df_customers.head())

    df_transactions.to_csv('transactions.csv', index=False)
    # print(len(df_transactions))
    df_customers.to_csv('city_totals.csv')
