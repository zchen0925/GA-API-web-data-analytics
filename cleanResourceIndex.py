import pandas as pd
import numpy as np 
import datetime

def import_file(path, csv_name):
    df = pd.read_csv(csv_name)
    return df

def normalizeDataFrame(df):
    df.drop(df.columns[[0,1,2,6,7,8,9,10,25,26,27,28,29,30,31,32,33,34,35,36,37]], axis = 1, inplace = True)
    # drop rows without url or publication date
    df.dropna(subset = ['urlTitle', 'publicationDate'])
    # add full path
    df.insert(loc = 2, column = 'FullURL', value = ('/resources/resource-library/' + df['urlTitle']))
    dates = df['publicationDate'].apply(datetime.datetime.strptime, args = ['%m/%d/%Y'])
    df.insert(loc = 4, column = 'PublishYear', value = dates.apply(lambda x: x.year))
    return df

def save_df_to_excel(df, path, file_name, sheet_name):
    writer = pd.ExcelWriter(path+file_name+'.xlsx')
    df.to_excel(writer, sheet_name=sheet_name)
    writer.save()

if __name__ == '__main__':
    df = import_file('D:\\CICW\\dotCMS GA\\', 'CICW - Resources_contents_8_14_2020.csv')
    df = normalizeDataFrame(df)
    save_df_to_excel(df, 'D:\\CICW\\dotCMS GA\\', file_name = "Resource Library Index", sheet_name = datetime.date.today().strftime("%m-%d-%y"))