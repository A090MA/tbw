# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 19:19:02 2019

@author: Nguyen7594
"""

import pandas as pd
import numpy as np

path = 'C:/Users/nguye/Documents/TTU/6339/group_project/dataset/ca_counties/csv_source/'
housing_price_csv = 'County_MedianValuePerSqft_AllHomes.csv'
education_csv = 'Education2017.csv'
poverty_csv = 'PovertyEstimates2017.csv'
unemployment_csv = 'UnemploymentReport2017Percentage.csv'
crime_csv = 'crime_rates_by_county_2017_per100k.csv'

## Import all files
# import the housing price - price per sqft
housing_pr = pd.read_csv(path+housing_price_csv,
                         names=['county_name','state',
                                'jan_17','feb_17','mar_17','apr_17','may_17','jun_17',
                                'jul_17','aug_17','sep_17','oct_17','nov_17','dec_17'],header=0)   
#housing_pr.head()

# import the education 
education2017 = pd.read_csv(path+education_csv,encoding = "ISO-8859-1",
                            names=['state','county_name',
                                'less_highschool','highschool','college','bachelor',
                                'less_highschool_perc','highschool_perc',
                                'college_perc','bachelor_perc'],header=0)  
#education2017.head() 

# import umemployment rate
unemployment2017 = pd.read_csv(path+unemployment_csv)  
#unemployment2017.head()
#unemployment2017.info()

# import the crime rate per 100,000
crime2017 = pd.read_csv(path+crime_csv,
                        names=['county_name','violent','murder',
                               'rape','robbery','aggrevated_assault',
                               'property','burglary','vehicle_theft','larceny_theft'],header=0)
#crime2017.head()


## Pre-data cleaning for each file
# housing price
cahousing_pr = housing_pr[housing_pr['state']=='CA']
cahousing_pr.drop('state',inplace=True,axis=1)
#cahousing_pr.head()
#cahousing_pr.count()
# housing price only available for 48 counties in California

# education2017
caeducation2017 = education2017[education2017['state']=='CA'][['county_name','less_highschool_perc','highschool_perc','college_perc','bachelor_perc']].reset_index(drop=True) 
caeducation2017.drop(0,inplace=True) 
#caeducation2017.head() 
#caeducation2017.count()
# there are all 58 available counties

# unemployment rate
caunemployment2017 = unemployment2017[['Name','2017','Median Household Income (2017)']]
caunemployment2017.rename(columns={'Median Household Income (2017)':'median_household_income',
                                   'Name':'county_name'},inplace=True)
caunemployment2017.drop(0,inplace=True) 
caunemployment2017['county_name'] = caunemployment2017['county_name'].map(lambda x: x[:-4])
#caunemployment2017.info()
#there are all 58 available counties


# crime rate per 100,000
crime2017['county_name'] = crime2017['county_name'] + ' County' 
#crime2017.info()


## Merge all 4 datasets
cacounties = cahousing_pr.merge(caeducation2017,how='outer',on='county_name')
cacounties = cacounties.merge(caunemployment2017,how='outer',on='county_name')
cacounties = cacounties.merge(crime2017,how='outer',on='county_name')
#cacounties.head()

