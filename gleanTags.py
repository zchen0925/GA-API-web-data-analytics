import pandas as pd
import numpy as np
import operator
import re


tagsIndex = pd.read_excel('D:\\CICW\\dotCMS GA\\Resource Library Index.xlsx', sheet_name = '08-15-20', header = 0)
tagsIndex = tagsIndex.loc[tagsIndex['PublishYear'] >= 2015]
def yearly_top10(df, year):
    df = df.loc[df['PublishYear'] == year]
    yearly_top10 = {}
    for rowValue in df['tags']:
        for tag in rowValue.split(','):
            if tag in yearly_top10:
                yearly_top10[tag] += 1
            else:
                yearly_top10[tag] = 1
    yearly_top10 = dict(sorted(yearly_top10.items(), key=operator.itemgetter(1), reverse=True)[:10])
    return yearly_top10

def clean_yearlytop10(df, year):
    df = df.loc[df['PublishYear'] == year]
    yearly_top10 = {}
    for rowValue in df['tags']:
        for tag in rowValue.split(','):
            if re.search("colloquium|symposium|covid-19|coronavirus|visuals for worship", tag) == None:
                if tag in yearly_top10:
                    yearly_top10[tag] += 1
                else:
                    yearly_top10[tag] = 1
    yearly_top10 = dict(sorted(yearly_top10.items(), key=operator.itemgetter(1), reverse=True)[:10])
    return yearly_top10

def save_df_to_excel(df, path, file_name, sheet_name):
    writer = pd.ExcelWriter(path+file_name+'.xlsx')
    df.to_excel(writer, sheet_name=sheet_name)
    writer.save()

def main():
    top_tags = []
    for year in range(2015, 2021):
        top_tags += (list(clean_yearlytop10(tagsIndex, year)))
    top_tags = list(set(top_tags))
    tagsDensity = pd.DataFrame()
    tagsDensity['Tags'] = top_tags
    for year in range(2015, 2021):
        tagsDensity[str(year)] = pd.Series
        yr_tags = []
        for iter in tagsIndex.loc[tagsIndex['PublishYear'] == year, 'tags']:
            yr_tags += iter.split(',')
        for tag in tagsDensity['Tags']:
            tag_yr_total = yr_tags.count(tag)
            tagsDensity.loc[tagsDensity['Tags'] == tag, str(year)] = tag_yr_total
    save_df_to_excel(tagsDensity, 'D:\\CICW\\dotCMS GA\\', "Tags Density Map_clean", sheet_name = '2015-2020')

if __name__ == '__main__':
  main()