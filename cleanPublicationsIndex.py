import pandas as pd
import numpy as np 
import datetime

def import_file(path, csv_name):
    df = pd.read_csv(csv_name)
    return df

def normalizeDataFrame(df):
    df.drop(df.columns[[0,1,2]], axis = 1, inplace = True)
    # drop rows without url or publication date
    df.dropna(subset = ['urlTitle'])
    # add full path
    df.insert(loc = 2, column = 'FullURL', value = ('/resources/publications/' + df['urlTitle']))
    dates = df['publishDate'].apply(datetime.datetime.strptime, args = ['%m/%d/%Y  %I:%M %p'])
    df.insert(loc = 6, column = 'PublishYear', value = dates.apply(lambda x: x.year))
    return df

def save_df_to_excel(df, path, file_name, sheet_name):
    writer = pd.ExcelWriter(path+file_name+'.xlsx')
    df.to_excel(writer, sheet_name=sheet_name)
    writer.save()

if __name__ == '__main__':
    df = import_file('D:\\CICW\\dotCMS GA\\', 'CICW - Publications_contents_8_14_2020.csv')
    df = normalizeDataFrame(df)
    save_df_to_excel(df, 'D:\\CICW\\dotCMS GA\\', file_name = "Resource Publications Index", sheet_name = datetime.date.today().strftime("%m-%d-%y"))