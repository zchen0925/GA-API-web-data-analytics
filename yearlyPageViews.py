"""Hello Analytics Reporting API V4."""

import pandas as pd
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'reporting API JSON key.json'
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
def get_report(analytics, startDate, endDate):
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
          'metrics': [{'expression': 'ga:pageviews'}, {'expression': 'ga:uniquepageviews'}],
          'dimensions': [{'name': 'ga:pagePath'}],
          #'orderBy': [{'fieldName': 'ga:pageviews'}],
          # if max size > 100,000, will only return the first 100,000 rows
          'pageSize': 100000
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
                # exclude pages outside resources/
                dict[header] = dimension

            for values in dateRangeValues:
                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    dict[metricHeader.get('name')] = int(value)

            list.append(dict)
    df = pd.DataFrame(list)
    # exclude all pages that reside outside of worship.calvin.edu/resources domain
    df = df.loc[df['ga:pagePath'].str.startswith("/resources")]

    # additional URL cleaning...
    df['ga:pagePath'] = df['ga:pagePath'].str.replace("/index.html.*", "", regex = True)
    df['ga:pagePath'] = df['ga:pagePath'].str.replace("/?mainFrame.*", "", regex = True)
    df['ga:pagePath'] = df['ga:pagePath'].str.replace("/listing.html.*", "", regex = True)
    df['ga:pagePath'] = df['ga:pagePath'].str.replace("/detail.html.*", "", regex = True)
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
    # merge pageviews for duplicate urls
    df = df.groupby(df['ga:pagePath']).sum()
    return df

def save_df_to_excel(df, path, file_name, sheet_name):
    writer = pd.ExcelWriter(path+file_name+'.xlsx')
    df.to_excel(writer, sheet_name=sheet_name)
    writer.save()

def main():
  analytics = initialize_analyticsreporting()
  dateRanges = [['2015-01-01','2015-12-31'],['2016-01-01','2016-12-31'],['2017-01-01','2017-12-31'],['2018-01-01','2018-12-31'],['2019-01-01','2019-12-31'],['2020-01-01','2020-06-30']]
  for i in range(0, 6):
      startDate = dateRanges[i][0]
      endDate = dateRanges[i][1]
      response = get_report(analytics, startDate, endDate)
      df = convert_todf(response)
      save_df_to_excel(df, 'D:\\CICW\\dotCMS GA\\', file_name = startDate[0:4]+" PageViews", sheet_name = startDate+" - "+endDate)

if __name__ == '__main__':
  main()
