# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 19:19:02 2019

@author: Nguyen7594
"""

import pandas as pd
import numpy as np
from re import sub
from sklearn import preprocessing
from sklearn.preprocessing import MinMaxScaler

path = 'D:/New folder (2)/6339/project/1/csv_source/'
housing_price_csv = 'County_MedianValuePerSqft_AllHomes.csv'
education_csv = 'Education2017.csv'
#poverty_csv = 'PovertyEstimates2017.csv'    NOT USED YET
unemployment_csv = 'UnemploymentReport2017Percentage.csv'
crime_csv = 'crime_rates_by_county_2017_per100k.csv'
airq_csv = 'aqireport2017.csv'

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

# import air quality
airq2017 = pd.read_csv(path+airq_csv,
                       names=['county_code','county_name',
                              'days_with_aqi','good','moderate','unhealthy_for_sensitive','unhealthy','very_unhealthy',
                              'aqi_max','aqi_90_perc','aqi_median',
                              'days_CO','days_NO2','days_O3','days_SO2','days_PM25','days_PM10'],header=0)

airq2017.info()



## Pre-data cleaning for each file
# housing price
cahousing_pr = housing_pr[housing_pr['state']=='CA']
cahousing_pr.drop('state',inplace=True,axis=1)
#cahousing_pr.head()
#cahousing_pr.count()

# education2017
caeducation2017 = education2017[education2017['state']=='CA'][['county_name','less_highschool_perc','highschool_perc','college_perc','bachelor_perc']].reset_index(drop=True) 
caeducation2017.drop(0,inplace=True) 
#caeducation2017.head() 
#caeducation2017.count()
# there are all 58 available counties

# unemployment rate
caunemployment2017 = unemployment2017[['Name','2017','Median Household Income (2017)']]
caunemployment2017.rename(columns={'Median Household Income (2017)':'median_household_income',
                                   'Name':'county_name',
                                   '2017':'unemployment_rate'},inplace=True)
caunemployment2017.drop(0,inplace=True) 
caunemployment2017['county_name'] = caunemployment2017['county_name'].map(lambda x: x[:-4])
#caunemployment2017.info()
#there are all 58 available counties


# crime rate per 100,000
crime2017['county_name'] = crime2017['county_name'] + ' County' 
#crime2017.info()

# air quality
caairq2017 = airq2017[['county_name','days_with_aqi','good']]
caairq2017['county_name'] = caairq2017['county_name'].map(lambda x: x[:-4])




## Merge all 4 datasets
cacounties = cahousing_pr.merge(caeducation2017,how='outer',on='county_name')
cacounties = cacounties.merge(caunemployment2017,how='outer',on='county_name')
cacounties = cacounties.merge(crime2017,how='outer',on='county_name')
cacounties = cacounties.merge(caairq2017,how='outer',on='county_name')
cacounties.info()


####--------------------------------------------------------------------------------------------####
## Data Cleaning ## 
# air quality only available for 53 counties, there are 5 missing values
cacounties['county_name'][cacounties['days_with_aqi'].isnull()]

                             
# housing price only available for 48 counties in California, 10 missing counties
months = ['jan_17','feb_17','mar_17','apr_17','may_17','jun_17','jul_17','aug_17','sep_17','oct_17','nov_17','dec_17']
cacounties['county_name'][cacounties[months].isnull().sum(1)>0]

#filling missing values for Housing price  
#based on population 2017, fill missing value by population:
#low-end population -> min price: Mono County, Inyo County, Del Norte County 
#25%-quantile of population -> 25%-quantile of price range: Tuolumne County, Tehama County, Mendocino County
#middle of population -> median price:  Imperial County
group1 = ['Mono County', 'Inyo County', 'Del Norte County']
group2 = ['Tuolumne County', 'Tehama County', 'Mendocino County']
group3 = ['Imperial County'] 
count = 1
for n in [group1,group2,group3]:
    for m in n:
        for i in ['jan_17','feb_17','mar_17','apr_17','may_17','jun_17','jul_17','aug_17','sep_17','oct_17','nov_17','dec_17']:
            if count == 1:  
                cacounties.loc[cacounties['county_name'] == m,i] = cacounties[i].min()
            elif count == 2:
                cacounties.loc[cacounties['county_name'] == m,i] = cacounties[i].quantile(0.25)
            else:
                cacounties.loc[cacounties['county_name'] == m,i] = cacounties[i].median()
    count += 1        
#double check the group 
cacounties.loc[cacounties['county_name'].isin(group1),months]

#for counties with missing values in air quality, we exclude them from our analysis: Alpine County, Modoc County, Sierra County, Yuba County, Lassen County  
cacounties.dropna(inplace=True)


#convert median_household_income type from object to be able for calculation    
cacounties['median_household_income'] = cacounties['median_household_income'].apply(lambda x: pd.to_numeric(sub(r'[^\d.]', '', x),errors='ignore'))
#convert violent, aggrevated_assault, from object to be able for calculation    
cacounties['violent'] = cacounties['violent'].apply(lambda x: pd.to_numeric(sub(r'[^\d.]', '', x),errors='ignore'))
cacounties['aggrevated_assault'] = cacounties['aggrevated_assault'].apply(lambda x: pd.to_numeric(sub(r'[^\d.]', '', x),errors='ignore'))
cacounties['property'] = cacounties['property'].apply(lambda x: pd.to_numeric(sub(r'[^\d.]', '', x),errors='ignore'))
cacounties['larceny_theft'] = cacounties['larceny_theft'].apply(lambda x: pd.to_numeric(sub(r'[^\d.]', '', x),errors='ignore'))


####------------------------------------------------------------------------------------------####
## Data Analysis for each area ##
## HOUSING PRICE ##
# average housing price for 2017
cacounties['ave_housingprice'] = cacounties[months].mean(1)
# top 10 average price:
# - Highest ave price
cacounties[['county_name','ave_housingprice']].sort_values(by=['ave_housingprice'],ascending=False)[:10]
# - Lowest, noted: there is a bias with the filling NA we did previous for 6 counties, thus we include additional 6 counties
cacounties[['county_name','ave_housingprice']].sort_values(by=['ave_housingprice'],ascending=True)[:16]
# percentage change of housing price in 12 months of 2017 = (last month - first month)/first month
cacounties['per_change_housingprice'] = (cacounties['dec_17']-cacounties['jan_17'])/(cacounties['jan_17'])*100
# top 10 percentage change of price:
# - fastest price
cacounties[['county_name','per_change_housingprice']].sort_values(by=['per_change_housingprice'],ascending=False)[:10]
# - slowest price
cacounties[['county_name','per_change_housingprice']].sort_values(by=['per_change_housingprice'],ascending=True)[:10]

## EDUCATION ##
# counties with highest/lowest percentage with degree of bachelor or higher
# counties with highest/lowest percentage without highschool graduation 
# grading_score = (less_highschool_perc+highschool_perc*2+college_perc*3+bachelor_perc*4)/10
cacounties['grading_score']=(cacounties['less_highschool_perc']+cacounties['highschool_perc']*2+cacounties['college_perc']*3+cacounties['bachelor_perc']*4)/10
cacounties[['county_name','grading_score']].sort_values(by=['grading_score'],ascending=False)[:10]
cacounties[['county_name','grading_score']].sort_values(by=['grading_score'],ascending=True)[:10]

## UNEMPLOYMENT RATE ##
# counties with highest/lowest unemployment rate
# counties with highest/lowest median housing income
# relationship between unemployment rate and median housing income
'''
## CRIME RATE ##
# divided by 100000
cacounties[['violent','murder','rape','robbery','aggrevated_assault','property','burglary','vehicle_theft','larceny_theft']] = cacounties[['violent','murder','rape','robbery','aggrevated_assault','property','burglary','vehicle_theft','larceny_theft']]/100000
# violent crime: violent, murder, aggrevated_assault, rape
# non-violent crime: robbery, property, burglary, vehicle_theft, larceny_theft
# Assumption: the records for these crimes are exclusive case
'''
##Air quality rate: Good days/days##
cacounties['Percentage_of_good_days'] = cacounties['good']/cacounties['days_with_aqi']
cacounties.info()

## Data Analysis for relationship/correlation between variables ## 
# Correlation matrix #
cor_matrix = cacounties.corr()

#housing_price vs other variables
cor_matrix['ave_housingprice']
#METHOD 1: assign value for ave_housingprice: HIGH (75%), MID (50%), MOD (25%), LOW (below 25%), based on quantiles
#Create categories column for ave_housingprice
labels_hp = ['Low_hp','Mod_hp','Mid_hp','High_hp']
cacounties['hprice_cat'] = pd.qcut(cacounties['ave_housingprice'],4,labels=labels_hp)
cacounties['hprice_cat'].value_counts()
cacounties.groupby('hprice_cat')[['violent','murder','rape','robbery','aggrevated_assault','property','burglary','vehicle_theft','larceny_theft']].mean()

#education vs unemployment rate
round(cor_matrix[['less_highschool_perc','bachelor_perc']],2)

#for index
def ComputeVals(row):
    return row['violent'] + row['property']
cacounties['crime_rate'] = cacounties.apply(ComputeVals, axis=1)
df = cacounties[['county_name', 'unemployment_rate', 'median_household_income',
                 'ave_housingprice', 'grading_score',
                 'Percentage_of_good_days', 'crime_rate']]
df.set_index('county_name', inplace = True)
del df.index.name
scaler = preprocessing.StandardScaler()
df_scaled = pd.DataFrame(scaler.fit_transform(df), columns=df.columns, index = df.index)
df_scaled

'''
To compute the index

for unemployment_rate and crime rate, the lower the better.
y = X1+X2+X3+X4+X5+X6
X1 = unemployment_rate          20%
X2 = median_household_income    30%
X3 = ave_housingprice           15%
X4 = grading_score              12%
X5 = Percentage_of_good_days    10%
X6 = crime_rate                 13%

y = -0.2*X1+0.25*X2+0.2*X3+0.1*X4+0.12*X5-0.13*X6
'''
def ComputeVals(row):
    return row['unemployment_rate']*-0.2 + row['median_household_income']*0.30+ row['ave_housingprice']*0.15 + row['grading_score']*0.12 + row['Percentage_of_good_days']*0.1 - row['crime_rate']*0.13
df_scaled['livability_index'] = df_scaled.apply(ComputeVals, axis=1)

file_out1 = 'index.csv'
df_scaled.to_csv(path + file_out1)




 
