import os
import zipfile
facts = {'store_sales', 'store_returns', 'catalog_sales',
         'catalog_returns', 'web_sales', 'web_returns', 'inventory'}
dimensions = {'store', 'call_center', 'catalog_page', 'web_site', 'web_page', 'warehouse',
              'customer', 'customer_address', 'customer_demographics', 'date_dim', 'ship_mode', 'item', 'promotion', 'reason', 'household_demographics', 'time_dim', 'income_band'}

files = ['query.sql', 'queries/query01.sql']
features = ['table-fact', 'table-dimension', 'subquery',
            'joins', 'ff join', 'df join', 'sum', 'max', 'count', 'avg', 'min', 'stddev_samp', 'where', 'largest where', 'group',  'largest group', 'order', 'largest order', 'limit', 'largest limit']


def get_files():

    with zipfile.ZipFile('./queries.zip', 'r') as zip_ref:
        zip_ref.extractall('./queries')
    return os.listdir('./queries')


if __name__ == '__main__':
    print(get_files())
