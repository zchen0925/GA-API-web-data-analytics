
"""Example for using the Google Search Analytics API (part of Search Console API).

Sample usage:

  $ python search_analytics_api_sample.py 'https://www.example.com/' '2015-05-01' '2015-05-30'

"""
from __future__ import print_function

import argparse
import sys
import datetime
from googleapiclient import sample_tools
import pandas as pd
from dateutil.relativedelta import *
from pprint import pprint 

'''
data available on and after 2019-04-22
'''

NUM_MONTHS = 6
'''
todo:

add function to convert response to pd df

add function to extract social promo and no promo lists of urls
'''

# Declare command-line flags.
argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument('property_uri', type=str,
                       help=('Site or app URI to query data for (including '
                             'trailing slash).'))
# argparser.add_argument('start_date', type=str,
#                        help=('Start date of the requested date range in '
#                              'YYYY-MM-DD format.'))
# argparser.add_argument('end_date', type=str,
#                        help=('End date of the requested date range in '
#                              'YYYY-MM-DD format.'))

paidPromoPages = pd.read_excel('D:\\CICW\\dotCMS GA\\Paid Promotion.xlsx', header = 0)
# paidPromoPages = 'http://worship.calvin.edu' + paidPromoPages['FullURL'].astype(str)
paidPromoPages = paidPromoPages['FullURL'].astype(str).tolist()

def get_urlposition_by_month(service, flags, pageURL, Date):
    Date = datetime.datetime.strptime(Date, "%Y-%m-%d")
    for i in range(NUM_MONTHS):
        startDate = Date + relativedelta(months = +i)
        endDate = Date + relativedelta(months = +(i+1))
        startDate = datetime.datetime.strftime(startDate, "%Y-%m-%d")
        endDate = datetime.datetime.strftime(endDate, "%Y-%m-%d")
        request = {
            'startDate': startDate,
            'endDate': endDate,
            'dimensions': ['page'],
            'dimensionFilterGroups': [{
                'filters': [{
                'dimension': 'page',
                'operator': 'contains',
                'expression': pageURL
                }]
            }],
            'rowLimit': 20
        }
        response = execute_request(service, flags.property_uri, request)
        print(startDate, "    ", endDate)
        print(pageURL)
        # print_table(response, 'page position by month')
        print(response)

    return response 

def main(argv):
    service, flags = sample_tools.init(
        argv, 'webmasters', 'v3', __doc__, __file__, parents=[argparser],
        scope='https://www.googleapis.com/auth/webmasters.readonly')

    for url in paidPromoPages: 
        get_urlposition_by_month(service, flags, url, '2020-01-01')
    
    


def execute_request(service, property_uri, request):
  return service.searchanalytics().query(
      siteUrl=property_uri, body=request).execute()


def print_table(response, title):
  """Prints out a response table.

  Each row contains key(s), clicks, impressions, CTR, and average position.

  Args:
    response: The server response to be printed as a table.
    title: The title of the table.
  """
  print('\n --' + title + ':')
  
  if 'rows' not in response:
    print('Empty response')
    return

  rows = response['rows']
  row_format = '{:<20}' + '{:>20}' * 4
  print(row_format.format('Keys', 'Clicks', 'Impressions', 'CTR', 'Position'))
  for row in rows:
    keys = ''
    # Keys are returned only if one or more dimensions are requested.
    if 'keys' in row:
      keys = u','.join(row['keys']).encode('utf-8').decode()
    print(row_format.format(
        keys, row['clicks'], row['impressions'], row['ctr'], row['position']))

if __name__ == '__main__':
  main(sys.argv)