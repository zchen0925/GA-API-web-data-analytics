import pandas as pd
import numpy as np
import operator
import re
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from dateutil.relativedelta import *

tagsIndex = pd.read_excel('D:\\CICW\\dotCMS GA\\Tags Density Map.xlsx', sheet_name = '2015-2020', header = 0)
tagsList = tagsIndex['Tags']

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'reporting API JSON key.json'
VIEW_ID = '6992300'

def get_yrURLS_with_tag(year, tag):
    df = pd.read_excel('D:\\CICW\\dotCMS GA\\Resource Library Index.xlsx', sheet_name = '08-15-20', header = 0)
    df['hasTopTags'] = df.loc[df['PublishYear'] == year, 'tags'].apply(lambda x: True if tag in x.split(',') else False)
    yrURLs = df.loc[(df['hasTopTags'] == True) & (df['PublishYear'] == year), 'FullURL'].tolist()
    return yrURLs

def initialize_analyticsreporting():
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)
  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)
  return analytics

#total sessions generatged from pages in given list(containing top tags) in given year
def get_tags_total(analytics, yrURLs, startDate, endDate):
    response = analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
          'metrics': [{'expression': 'ga:sessions'}],
          'dimensions': [{'name': 'ga:pagePath'}],
          'orderBys': [{'fieldName': 'ga:sessions', 'sortOrder': 'DESCENDING'}],
          'dimensionFilterClauses':[{'filters':[{'dimensionName': 'ga:pagePath', 'operator': "IN_LIST", 'expressions': yrURLs}]}],
          'pageSize': 100000,
        }]
      }
    ).execute()
    #returns single total
    for report in response.get('reports', []):
        for totals in report.get('data', {}).get('totals', []):
            for value in totals.get('values'):
                return int(value)

def yr_sessions_total(analytics, startDate, endDate):
    response = analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
          'metrics': [{'expression': 'ga:sessions'}],
          'orderBys': [{'fieldName': 'ga:sessions', 'sortOrder': 'DESCENDING'}],
          'pageSize': 100000,
        }]
      }
    ).execute()
    for report in response.get('reports', []):
        for totals in report.get('data', {}).get('totals', []):
            for value in totals.get('values'):
                return int(value)

def save_df_to_excel(df, path, file_name, sheet_name):
    writer = pd.ExcelWriter(path+file_name+'.xlsx')
    df.to_excel(writer, sheet_name=sheet_name)
    writer.save()

def main():
    analytics = initialize_analyticsreporting()
    tagsPerformance = pd.DataFrame()
    tagsPerformance['Tags'] = tagsList
    yr_sessionstotals = {}
    for year in range(2015, 2021):
        tagsPerformance[str(year)] = np.nan
        startDate = str(year)+"-01-01"
        endDate = str(year)+'-12-31'
        yr_sessionstotals[year] = yr_sessions_total(analytics, startDate, endDate)
        
        for tag in tagsList:
            yrURLs = get_yrURLS_with_tag(year, tag)
            if (len(yrURLs) > 0):
                tag_yr_total = get_tags_total(analytics, yrURLs, startDate, endDate)
                tagsPerformance.loc[tagsPerformance['Tags'] == tag, str(year)] = tag_yr_total

    tagsPerformance.fillna(0, inplace = True)
    save_df_to_excel(tagsPerformance, 'D:\\CICW\\dotCMS GA\\', "Tags Traffic Performance", sheet_name = '2015-2020')

if __name__ == '__main__':
  main()
