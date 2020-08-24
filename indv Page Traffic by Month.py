"""Hello Analytics Reporting API V4."""

import sys
import re
import time
import datetime
import pandas as pd
import numpy as np
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from dateutil.relativedelta import *

NUM_MONTHS = 10
SAMPLE_SIZE = 3


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
#JSON key pair generated for cicw-seo@dotcms-seo-graphs-sandbox.iam.gserviceaccount.com
KEY_FILE_LOCATION = 'reporting API JSON key.json'
#the "Main" view under "www.calvin.edu/worship" property
VIEW_ID = '6992300'


def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics

#reporting on pageviews for each unique page url in the given time frame 
def get_report(analytics, pagePath, startDate, endDate):
  """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
          'metrics': [{'expression': 'ga:uniquepageviews'}],
          'dimensions': [{'name': 'ga:pagePath'}, {'name': 'ga:source'}, {'name':'ga:medium'}],
          'orderBys': [{'fieldName': 'ga:uniquepageviews', 'sortOrder': 'DESCENDING'}],
          'dimensionFilterClauses':[{'filters':[{'dimensionName': 'ga:pagePath', 'expressions': pagePath}]}],
          'pageSize': 100000,
        }]
      }
  ).execute()


def convert_todf(response):
    """Parses and prints the Analytics Reporting API V4 response.

    Args:
        response: An Analytics Reporting API V4 response.
    """
    list = []
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get(
            'metricHeader', {}).get('metricHeaderEntries', [])

        for row in report.get('data', {}).get('rows', []):
            dict = {}
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            for header, dimension in zip(dimensionHeaders, dimensions):
              dimension = str(dimension)
              dict[header] = dimension

            for values in dateRangeValues:
                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    dict[metricHeader.get('name')] = int(value)
            list.append(dict)
    df = pd.DataFrame(list)

    if df.empty == False:
    # additional URL cleaning...
      df['ga:pagePath'] = df['ga:pagePath'].str.replace("/index.html.*", "", regex = True)
      df['ga:pagePath'] = df['ga:pagePath'].str.replace("/?mainFrame.*", "", regex = True)
      df['ga:pagePath'] = df['ga:pagePath'].str.replace("/listing.html.*", "", regex = True)
      df['ga:pagePath'] = df['ga:pagePath'].str.replace("/detail.html.*", "", regex = True)
      df['ga:pagePath'] = df['ga:pagePath'].str.replace("/null.*", "", regex = True)
      # remove components beginning with special characters 
      for c in ['\$', '&', '#', '\?', '\+','@', '=', ':', '%', '!']:
        c = c+'.*'
        df['ga:pagePath'] = df['ga:pagePath'].str.replace(c, '', regex = True)
      # remove dash at end
      df['ga:pagePath'] = df['ga:pagePath'].str.replace('/$', '', regex = True)
      # bug in GA scraping of pathPaths? remove duplicate full url at end
      df['ga:pagePath'] = df['ga:pagePath'].str.replace("http.*", "", regex = True)
      # seems to be an older migration issue
      df['ga:pagePath'] = df['ga:pagePath'].str.replace("^/resources-library", "/resources/resource-library", regex = True)
      df = df[df['ga:medium'] != '(not set)']
    return df


def save_df_to_excel(df, path, file_name, sheet_name):
    writer = pd.ExcelWriter(path+file_name+'.xlsx')
    df.to_excel(writer, sheet_name=sheet_name)
    writer.save()

def main():
  analytics = initialize_analyticsreporting()
  monthlyList = []
  pagesIndex = pd.read_excel('D:\\CICW\\dotCMS GA\\Resource Library Index.xlsx', sheet_name = '08-15-20', header = 0)
  pagesIndex = pagesIndex.loc[pagesIndex['PublishYear'] == 2019].sample(SAMPLE_SIZE * 10)
  #each resource url
  for i in range(SAMPLE_SIZE):
    URL_LIST = pagesIndex['FullURL'].tolist()[i * 10 : (i + 1)* 10]
    PUBLISH_DATES = pagesIndex['publicationDate'].tolist()[i * 10 : (i + 1)* 10]
    for URL, DATE in zip(URL_LIST, PUBLISH_DATES):
      DATE = datetime.datetime.strptime(DATE, "%m/%d/%Y")
      df = pd.DataFrame()
      #each month
      for i in range(NUM_MONTHS):
        startDate = DATE + relativedelta(months = +i)
        endDate = DATE + relativedelta(months = +(i+1))
        response = get_report(analytics, URL, datetime.datetime.strftime(startDate, "%Y-%m-%d"), datetime.datetime.strftime(endDate, "%Y-%m-%d"))
        df = convert_todf(response)
        if df.empty == False:
          df['monthsSince'] = i+1
          monthlyList.append(df)
          # print("URL:  "+URL[28:38]+"  month:  %d    monthlyTotal: %d" %(i, df['ga:uniquepageviews'].sum()))
    time.sleep(125)
  df = pd.concat(monthlyList)
  #if source is from social media, update medium from referral to social media
  df['ifsocial'] = df['ga:source'].apply(lambda x: True if re.search('facebook|twitter|youtube', x) else False) 
  df.loc[df['ifsocial'] == True, 'ga:medium'] = 'socialmedia'
  #if medium is email or promo, aggregate to programpromo
  df.loc[df['ga:medium'] == ('email'), 'ga:medium'] = 'programpromo'
  df.loc[df['ga:medium'] == ('promo'), 'ga:medium'] = 'programpromo'
  df.loc[df['ga:medium'] == ('(none)'), 'ga:medium'] = 'direct'
  df.drop(columns = ['ifsocial'], inplace = True)
  #aggregate
  df = df.groupby(['monthsSince', 'ga:medium'], as_index = False).sum()

  # add zero value rows and monthly totals
  monthlyTotal = []
  for i in range(1, NUM_MONTHS+1):
    for m in df['ga:medium']:
      monthly = 0
      if ((df['monthsSince'] == i) & (df['ga:medium'] == m)).any() == False:
        # print('row missing, current pd rows: %d' % (len(df)))
        df = df.append({'ga:medium':	m, 'monthsSince' : i,	'ga:uniquepageviews' : 0}, ignore_index = True)
        # print('after appending  ', m, '  and monthsSince: ', i, '  current pd rows: %d' % (len(df)))
    #find value of monthly total    
    monthly = df.loc[df['monthsSince'] == i, 'ga:uniquepageviews'].sum()
    #append to list
    rows_permonth = df['ga:medium'].nunique()
    monthlyTotal += rows_permonth * [monthly]
  df.sort_values(by = ['monthsSince'], inplace = True)
  df['monthlyTotal'] = monthlyTotal
  df['monthlyPercentage'] = df['ga:uniquepageviews'] / df['monthlyTotal']
  save_df_to_excel(df, 'D:\\CICW\\dotCMS GA\\', "Monthly Traffic by Medium Random Sample", sheet_name = 'Resources Published in 2019')

if __name__ == '__main__':
  main()
