# import necessary libraries
import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

# change working directory to project folder
import os
os.chdir('/Users/lacm/Documents/Stanford/*Capstone Research/Data')

# import ccd data (longitudinal)
ccdschools_unclean = pd.read_stata('/Users/lacm/Dropbox/segregation lab/data and programs/data/ccd/imputed school ccd data/segregation lab imputed files/clean school by year imputed data combined collapsed.dta') 
ccdschools = ccdschools_unclean

# CLEAN CCD DATA

# convert variables to correct type
ccdschools['year'] = ccdschools['year'].astype('str').str[:4]
ccdschools['leaid'] = ccdschools['leaid'].astype('str')
ccdschools['geolea'] = ccdschools['geolea'].astype('str')
ccdschools['leaid'] = ccdschools['leaid'].astype('str')
ccdschools['ncessch'] = ccdschools['ncessch'].astype('str')
ccdschools['nces7_final'] = ccdschools['nces7_final'].astype('str')
ccdschools['charter'] = ccdschools['charter'].astype('int')
ccdschools['virtual'] = ccdschools['virtual'].astype('float')
ccdschools['magnet'] = ccdschools['magnet'].astype('int')
ccdschools['type'] = ccdschools['type'].astype('int')
ccdschools['grdlo'] = ccdschools['grdlo'].astype('float')
ccdschools['grdhi'] = ccdschools['grdhi'].astype('float')

# strip string columns
for column in ccdschools.columns:
    if ccdschools.dtypes[column] == 'object':
        ccdschools[column] = ccdschools[column].str.strip()

# add leading 0 to geolea, leaid, and school ids where it was chopped off from incorrect variable type
ccdschools.loc[ccdschools['geolea'].str.len() == 6, 'geolea'] = '0' + ccdschools['geolea']
ccdschools.loc[ccdschools['leaid'].str.len() == 6, 'leaid'] = '0' + ccdschools['leaid']
ccdschools.loc[ccdschools['ncessch'].str.len() == 11, 'ncessch'] = '0' + ccdschools['ncessch']
ccdschools.loc[ccdschools['nces7_final'].str.len() == 11, 'nces7_final'] = '0' + ccdschools['nces7_final']

# filter for only years with SABS/SABINS data; rename years so that year corresponds to second year of school year
ccdschools = ccdschools[(ccdschools.year == '1999') | (ccdschools.year == '2009') | (ccdschools.year == '2010') | 
                        (ccdschools.year == '2011') | (ccdschools.year == '2013') | (ccdschools.year == '2015')]

ccdschools['year'] = ccdschools['year'].replace(['1999', '2009', '2010', '2011', '2013', '2015'], 
                                                ['2000', '2010', '2011', '2012', '2014', '2016'])

# keep only relevant columns
ccdschools = ccdschools[['geolea','leaid', 'ncessch','nces7_final', 'year', 'state_name', 'county_name',  'grdlo', 
                         'grdhi', 'charter', 'magnet', 'virtual', 'type']]

# rename geolea column
ccdschools = ccdschools.rename(columns={"geolea" : "geoleaid"})

# filter for only schools that serve 3rd grade
ccdschools = ccdschools[(ccdschools.grdlo <= 3) & (ccdschools.grdhi >= 3)]

# create separate ccd files for each year using SABS data (want to filter out virtual, magnet, and non-regular schools there directly)
ccdschools_2011_unclean = ccdschools[ccdschools.year == '2011']
ccdschools_2014_unclean = ccdschools[ccdschools.year == '2014']
ccdschools_2016_unclean = ccdschools[ccdschools.year == '2016']

# filter out virtual schools
ccdschools = ccdschools[ccdschools.virtual != 1]

# filter out magnet schools (only want schools with true catchment zones)
ccdschools = ccdschools[ccdschools.magnet == 0]

# keep only schools of type regular
# school types: 1 = regular, 2 = special education, 3 = vocational, 4 = alternative/other, 5 = ?
ccdschools = ccdschools[ccdschools.type == 1]

# creating separate CCD file for each year after full ccd dataset is clean

ccdschools_2000 = ccdschools[ccdschools.year == '2000']
ccdschools_2010 = ccdschools[ccdschools.year == '2010']
ccdschools_2011 = ccdschools[ccdschools.year == '2011']
ccdschools_2012 = ccdschools[ccdschools.year == '2012']
ccdschools_2014 = ccdschools[ccdschools.year == '2014']
ccdschools_2016 = ccdschools[ccdschools.year == '2016']

# SY 1999-2000

# import excel file of all schools in the 2010-11 data year
sabs2000_baltimore_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Baltimore_City_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_baltimorecounty_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Baltimore_County_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_broward_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Broward_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_chicago_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Chicago_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_clark_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Clark_County_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_dallas_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Dallas_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_detroit_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Detroit_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_duval_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Duval_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_fairfax_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Fairfax_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_hillsborough_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Hillsborough_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_houston_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Houston_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_la_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_LA_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_miamidade_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Miami_Dade_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_milwaukee_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Milwaukee_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_montgomery_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Montgomery_Cnty_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_orange_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Orange_Cnty_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_palmbeach_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Palm_Beach_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_philadelphia_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Philadelphia_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_pinellas_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Pinellas_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_princegeorge_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_Prince_George_Cnty_fixed_geometries_reprojected.xlsx', header=0)
sabs2000_sandiego_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Sal Saporito Data/Clean/elem_San_Diego_fixed_geometries_reprojected.xlsx', header=0)

# clean baltimore city data
sabs2000_baltimore = sabs2000_baltimore_unclean

# make column names lower
sabs2000_baltimore = sabs2000_baltimore.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_baltimore['ccd_id'] = sabs2000_baltimore['ccd_id'].astype(str)
sabs2000_baltimore.loc[sabs2000_baltimore['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_baltimore['ccd_id']
sabs2000_baltimore.loc[sabs2000_baltimore['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_baltimore['ccd_id']

# strip string columns
for column in sabs2000_baltimore.columns:
    if sabs2000_baltimore.dtypes[column] == 'object':
        sabs2000_baltimore[column] = sabs2000_baltimore[column].str.strip()
        
# add geoleaid for baltimore city schools
sabs2000_baltimore['geoleaid'] = '2400090'
sabs2000_baltimore['ncessch'] = sabs2000_baltimore['geoleaid'] + sabs2000_baltimore['ccd_id']

# merge baltimore city sabs dataset with ccd 2000 record of schools to find baltimore city schools that serve 3rd grade
sabs2000_baltimore = sabs2000_baltimore.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_baltimore = sabs2000_baltimore[(sabs2000_baltimore['grdlo'].notnull()) | (sabs2000_baltimore['grdhi'].notnull())]

# clean baltimore county data
sabs2000_baltimorecounty = sabs2000_baltimorecounty_unclean

# make column names lower
sabs2000_baltimorecounty = sabs2000_baltimorecounty.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_baltimorecounty['ccd_id'] = sabs2000_baltimorecounty['ccd_id'].astype(str)
sabs2000_baltimorecounty.loc[sabs2000_baltimorecounty['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_baltimorecounty['ccd_id']
sabs2000_baltimorecounty.loc[sabs2000_baltimorecounty['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_baltimorecounty['ccd_id']

# strip string columns
for column in sabs2000_baltimorecounty.columns:
    if sabs2000_baltimorecounty.dtypes[column] == 'object':
        sabs2000_baltimorecounty[column] = sabs2000_baltimorecounty[column].str.strip()

# add geoleaid for baltimore county schools
sabs2000_baltimorecounty['geoleaid'] = '2400120'
sabs2000_baltimorecounty['ncessch'] = sabs2000_baltimorecounty['geoleaid'] + sabs2000_baltimorecounty['ccd_id']
sabs2000_baltimorecounty


# merge baltimore county sabs dataset with ccd 2000 record of schools to find baltimore county schools that serve 3rd grade
sabs2000_baltimorecounty = sabs2000_baltimorecounty.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_baltimorecounty = sabs2000_baltimorecounty[(sabs2000_baltimorecounty['grdlo'].notnull()) | (sabs2000_baltimorecounty['grdhi'].notnull())]

# clean broward county data
sabs2000_broward = sabs2000_broward_unclean

# make column names lower
sabs2000_broward = sabs2000_broward.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_broward['ccd_id'] = sabs2000_broward['ccd_id'].astype(str)
sabs2000_broward.loc[sabs2000_broward['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_broward['ccd_id']
sabs2000_broward.loc[sabs2000_broward['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_broward['ccd_id']

# strip string columns
for column in sabs2000_broward.columns:
    if sabs2000_broward.dtypes[column] == 'object':
        sabs2000_broward[column] = sabs2000_broward[column].str.strip()

# add geoleaid for broward schools
sabs2000_broward['geoleaid'] = '1200180'
sabs2000_broward['ncessch'] = sabs2000_broward['geoleaid'] + sabs2000_broward['ccd_id']

# merge broward sabs dataset with ccd 2000 record of schools to find broward schools that serve 3rd grade
sabs2000_broward = sabs2000_broward.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_broward = sabs2000_broward[(sabs2000_broward['grdlo'].notnull()) | (sabs2000_broward['grdhi'].notnull())]

# clean chicago data
sabs2000_chicago = sabs2000_chicago_unclean

# make column names lower
sabs2000_chicago = sabs2000_chicago.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_chicago['ccd_id'] = sabs2000_chicago['ccd_id'].astype(str)
sabs2000_chicago['ccd_id'] = sabs2000_chicago['ccd_id'].str.rstrip('.0')

sabs2000_chicago.loc[sabs2000_chicago['ccd_id'].str.len() == 1, 'ccd_id'] = '0000' + sabs2000_chicago['ccd_id']
sabs2000_chicago.loc[sabs2000_chicago['ccd_id'].str.len() == 2, 'ccd_id'] = '000' + sabs2000_chicago['ccd_id']
sabs2000_chicago.loc[sabs2000_chicago['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_chicago['ccd_id']
sabs2000_chicago.loc[sabs2000_chicago['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_chicago['ccd_id']
sabs2000_chicago['ccd_id'] = sabs2000_chicago['ccd_id'].replace(['00nan'], [''])

# strip string columns
for column in sabs2000_chicago.columns:
    if sabs2000_chicago.dtypes[column] == 'object':
        sabs2000_chicago[column] = sabs2000_chicago[column].str.strip()

# add geoleaid for chicago schools
sabs2000_chicago['geoleaid'] = '1709930'
sabs2000_chicago['ncessch'] = sabs2000_chicago['geoleaid'] + sabs2000_chicago['ccd_id']

# merge chicago sabs dataset with ccd 2000 record of schools to find chicago schools that serve 3rd grade
sabs2000_chicago = sabs2000_chicago.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_chicago = sabs2000_chicago[(sabs2000_chicago['grdlo'].notnull()) | (sabs2000_chicago['grdhi'].notnull())]

# clean clark county data 
sabs2000_clark = sabs2000_clark_unclean

# make column names lower
sabs2000_clark = sabs2000_clark.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_clark['ccd_id'] = sabs2000_clark['ccd_id'].astype(str)
sabs2000_clark.loc[sabs2000_clark['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_clark['ccd_id']
sabs2000_clark.loc[sabs2000_clark['ccd_id'].str.len() == 2, 'ccd_id'] = '000' + sabs2000_clark['ccd_id']
sabs2000_clark.loc[sabs2000_clark['ccd_id'].str.len() == 1, 'ccd_id'] = '0000' + sabs2000_clark['ccd_id']

# strip string columns
for column in sabs2000_clark.columns:
    if sabs2000_clark.dtypes[column] == 'object':
        sabs2000_clark[column] = sabs2000_clark[column].str.strip()

# add geoleaid for clark schools
sabs2000_clark['geoleaid'] = '3200060'
sabs2000_clark['ncessch'] = sabs2000_clark['geoleaid'] + sabs2000_clark['ccd_id'].astype(str)

# merge clark sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_clark = sabs2000_clark.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_clark = sabs2000_clark[(sabs2000_clark['grdlo'].notnull()) | (sabs2000_clark['grdhi'].notnull())]

# clean dallas data
sabs2000_dallas = sabs2000_dallas_unclean

# make column names lower
sabs2000_dallas = sabs2000_dallas.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_dallas['ccd_id'] = sabs2000_dallas['ccd_id'].astype(str)
sabs2000_dallas['ccd_id'] = sabs2000_dallas['ccd_id'].str.rstrip('.0')

sabs2000_dallas.loc[sabs2000_dallas['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_dallas['ccd_id']
sabs2000_dallas.loc[sabs2000_dallas['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_dallas['ccd_id']
sabs2000_dallas.loc[sabs2000_dallas['ccd_id'].str.len() == 2, 'ccd_id'] = '000' + sabs2000_dallas['ccd_id']
sabs2000_dallas['ccd_id'] = sabs2000_dallas['ccd_id'].replace(['00nan'], [''])

# strip string columns
for column in sabs2000_dallas.columns:
    if sabs2000_dallas.dtypes[column] == 'object':
        sabs2000_dallas[column] = sabs2000_dallas[column].str.strip()

# add geoleaid for dallas schools
sabs2000_dallas['geoleaid'] = '4816230'
sabs2000_dallas['ncessch'] = sabs2000_dallas['geoleaid'] + sabs2000_dallas['ccd_id'].astype(str)

# merge dallas sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_dallas = sabs2000_dallas.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_dallas = sabs2000_dallas[(sabs2000_dallas['grdlo'].notnull()) | (sabs2000_dallas['grdhi'].notnull())]

# clean detroit data
sabs2000_detroit = sabs2000_detroit_unclean

# make column names lower
sabs2000_detroit = sabs2000_detroit.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_detroit['ccd_id'] = sabs2000_detroit['ccd_id'].astype(str)
sabs2000_detroit['ccd_id'] = sabs2000_detroit['ccd_id'].str.rstrip('.0')

sabs2000_detroit.loc[sabs2000_detroit['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_detroit['ccd_id']
sabs2000_detroit.loc[sabs2000_detroit['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_detroit['ccd_id']
sabs2000_detroit.loc[sabs2000_detroit['ccd_id'].str.len() == 2, 'ccd_id'] = '000' + sabs2000_detroit['ccd_id']

# strip string columns
for column in sabs2000_detroit.columns:
    if sabs2000_detroit.dtypes[column] == 'object':
        sabs2000_detroit[column] = sabs2000_detroit[column].str.strip()

# add geoleaid for detroit schools
sabs2000_detroit['geoleaid'] = '2601103'
sabs2000_detroit['ncessch'] = sabs2000_detroit['geoleaid'] + sabs2000_detroit['ccd_id'].astype(str)

# merge detroit sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_detroit = sabs2000_detroit.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_detroit = sabs2000_detroit[(sabs2000_detroit['grdlo'].notnull()) | (sabs2000_detroit['grdhi'].notnull())]

# clean duval county data
sabs2000_duval = sabs2000_duval_unclean

# make column names lower
sabs2000_duval = sabs2000_duval.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_duval['ccd_id'] = sabs2000_duval['ccd_id'].astype(str)
sabs2000_duval.loc[sabs2000_duval['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_duval['ccd_id']
sabs2000_duval.loc[sabs2000_duval['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_duval['ccd_id']

# strip string columns
for column in sabs2000_duval.columns:
    if sabs2000_duval.dtypes[column] == 'object':
        sabs2000_duval[column] = sabs2000_duval[column].str.strip()

# add geoleaid for baltimore city schools
sabs2000_duval['geoleaid'] = '1200480'
sabs2000_duval['ncessch'] = sabs2000_duval['geoleaid'] + sabs2000_duval['ccd_id'].astype(str)

# merge duval sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_duval = sabs2000_duval.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_duval = sabs2000_duval[(sabs2000_duval['grdlo'].notnull()) | (sabs2000_duval['grdhi'].notnull())]

# clean fairfax county data
sabs2000_fairfax = sabs2000_fairfax_unclean

# make column names lower
sabs2000_fairfax = sabs2000_fairfax.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_fairfax['ccd_id'] = sabs2000_fairfax['ccd_id'].astype(str)
sabs2000_fairfax.loc[sabs2000_fairfax['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_fairfax['ccd_id']
sabs2000_fairfax.loc[sabs2000_fairfax['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_fairfax['ccd_id']
sabs2000_fairfax.loc[sabs2000_fairfax['ccd_id'].str.len() == 2, 'ccd_id'] = '000' + sabs2000_fairfax['ccd_id']

# strip string columns
for column in sabs2000_fairfax.columns:
    if sabs2000_fairfax.dtypes[column] == 'object':
        sabs2000_fairfax[column] = sabs2000_fairfax[column].str.strip()

# add geoleaid for fairfax schools
sabs2000_fairfax['geoleaid'] = '5101260'
sabs2000_fairfax['ncessch'] = sabs2000_fairfax['geoleaid'] + sabs2000_fairfax['ccd_id'].astype(str)

# merge fairfax sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_fairfax = sabs2000_fairfax.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_fairfax = sabs2000_fairfax[(sabs2000_fairfax['grdlo'].notnull()) | (sabs2000_fairfax['grdhi'].notnull())]

# clean hillsborough data
sabs2000_hillsborough = sabs2000_hillsborough_unclean

# make column names lower
sabs2000_hillsborough = sabs2000_hillsborough.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_hillsborough['ccd_id'] = sabs2000_hillsborough['ccd_id'].astype(str)
sabs2000_hillsborough.loc[sabs2000_hillsborough['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_hillsborough['ccd_id']
sabs2000_hillsborough.loc[sabs2000_hillsborough['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_hillsborough['ccd_id']

# strip string columns
for column in sabs2000_hillsborough.columns:
    if sabs2000_hillsborough.dtypes[column] == 'object':
        sabs2000_hillsborough[column] = sabs2000_hillsborough[column].str.strip()

# add geoleaid for hillsborough schools
sabs2000_hillsborough['geoleaid'] = '1200870'
sabs2000_hillsborough['ncessch'] = sabs2000_hillsborough['geoleaid'] + sabs2000_hillsborough['ccd_id'].astype(str)

# merge hillsborough sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_hillsborough = sabs2000_hillsborough.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_hillsborough = sabs2000_hillsborough[(sabs2000_hillsborough['grdlo'].notnull()) | (sabs2000_hillsborough['grdhi'].notnull())]

# clean houston data
sabs2000_houston = sabs2000_houston_unclean

# make column names lower
sabs2000_houston = sabs2000_houston.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_houston['ccd_id'] = sabs2000_houston['ccd_id'].astype(str)
sabs2000_houston['ccd_id'] = sabs2000_houston['ccd_id'].str.rstrip('.0')

sabs2000_houston.loc[sabs2000_houston['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_houston['ccd_id']
sabs2000_houston.loc[sabs2000_houston['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_houston['ccd_id']
sabs2000_houston.loc[sabs2000_houston['ccd_id'].str.len() == 2, 'ccd_id'] = '000' + sabs2000_houston['ccd_id']
sabs2000_houston['ccd_id'] = sabs2000_houston['ccd_id'].replace(['00nan'], [''])


# strip string columns
for column in sabs2000_houston.columns:
    if sabs2000_houston.dtypes[column] == 'object':
        sabs2000_houston[column] = sabs2000_houston[column].str.strip()

# add geoleaid for houston schools
sabs2000_houston['geoleaid'] = '4807710'
sabs2000_houston['ncessch'] = sabs2000_houston['geoleaid'] + sabs2000_houston['ccd_id'].astype(str)

# merge houston sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_houston = sabs2000_houston.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_houston = sabs2000_houston[(sabs2000_houston['grdlo'].notnull()) | (sabs2000_houston['grdhi'].notnull())]

# clean los angeles data
sabs2000_la = sabs2000_la_unclean

# make column names lower
sabs2000_la = sabs2000_la.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_la['ccd_id'] = sabs2000_la['ccd_id'].astype(str)
sabs2000_la['ccd_id'] = sabs2000_la['ccd_id'].str.rstrip('.0')

sabs2000_la.loc[sabs2000_la['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_la['ccd_id']
sabs2000_la.loc[sabs2000_la['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_la['ccd_id']
sabs2000_la.loc[sabs2000_la['ccd_id'].str.len() == 2, 'ccd_id'] = '000' + sabs2000_la['ccd_id']
sabs2000_la.loc[sabs2000_la['ccd_id'].str.len() == 1, 'ccd_id'] = '0000' + sabs2000_la['ccd_id']
sabs2000_la['ccd_id'] = sabs2000_la['ccd_id'].replace(['00nan'], [''])

# strip string columns
for column in sabs2000_la.columns:
    if sabs2000_la.dtypes[column] == 'object':
        sabs2000_la[column] = sabs2000_la[column].str.strip()

# add geoleaid for los angeles schools
sabs2000_la['geoleaid'] = '0622710'
sabs2000_la['ncessch'] = sabs2000_la['geoleaid'] + sabs2000_la['ccd_id'].astype(str)

# merge los angeles sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_la = sabs2000_la.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_la = sabs2000_la[(sabs2000_la['grdlo'].notnull()) | (sabs2000_la['grdhi'].notnull())]

# clean miami data
sabs2000_miamidade = sabs2000_miamidade_unclean

# make column names lower
sabs2000_miamidade = sabs2000_miamidade.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_miamidade['ccd_id'] = sabs2000_miamidade['ccd_id'].astype(str)
sabs2000_miamidade['ccd_id'] = sabs2000_miamidade['ccd_id'].str.rstrip('.0')

sabs2000_miamidade.loc[sabs2000_miamidade['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_miamidade['ccd_id']
sabs2000_miamidade.loc[sabs2000_miamidade['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_miamidade['ccd_id']
sabs2000_miamidade.loc[sabs2000_miamidade['ccd_id'].str.len() == 2, 'ccd_id'] = '000' + sabs2000_miamidade['ccd_id']
sabs2000_miamidade.loc[sabs2000_miamidade['ccd_id'].str.len() == 1, 'ccd_id'] = '0000' + sabs2000_miamidade['ccd_id']
sabs2000_miamidade['ccd_id'] = sabs2000_miamidade['ccd_id'].replace(['00nan'], [''])

# strip string columns
for column in sabs2000_miamidade.columns:
    if sabs2000_miamidade.dtypes[column] == 'object':
        sabs2000_miamidade[column] = sabs2000_miamidade[column].str.strip()

# add geoleaid for miami schools
sabs2000_miamidade['geoleaid'] = '1200390'
sabs2000_miamidade['ncessch'] = sabs2000_miamidade['geoleaid'] + sabs2000_miamidade['ccd_id'].astype(str)

# merge miami sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_miamidade = sabs2000_miamidade.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_miamidade = sabs2000_miamidade[(sabs2000_miamidade['grdlo'].notnull()) | (sabs2000_miamidade['grdhi'].notnull())]

# clean milwaukee data
sabs2000_milwaukee = sabs2000_milwaukee_unclean

# make column names lower
sabs2000_milwaukee = sabs2000_milwaukee.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_milwaukee['ccd_id'] = sabs2000_milwaukee['ccd_id'].astype(str)
sabs2000_milwaukee['ccd_id'] = sabs2000_milwaukee['ccd_id'].str.rstrip('.0')

sabs2000_milwaukee.loc[sabs2000_milwaukee['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_milwaukee['ccd_id']
sabs2000_milwaukee.loc[sabs2000_milwaukee['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_milwaukee['ccd_id']
sabs2000_milwaukee.loc[sabs2000_milwaukee['ccd_id'].str.len() == 2, 'ccd_id'] = '000' + sabs2000_milwaukee['ccd_id']
sabs2000_milwaukee['ccd_id'] = sabs2000_milwaukee['ccd_id'].replace(['00nan'], [''])

# strip string columns
for column in sabs2000_milwaukee.columns:
    if sabs2000_milwaukee.dtypes[column] == 'object':
        sabs2000_milwaukee[column] = sabs2000_milwaukee[column].str.strip()

# add geoleaid for milwaukee schools
sabs2000_milwaukee['geoleaid'] = '5509600'
sabs2000_milwaukee['ncessch'] = sabs2000_milwaukee['geoleaid'] + sabs2000_milwaukee['ccd_id'].astype(str)

# merge milwaukee sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_milwaukee = sabs2000_milwaukee.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_milwaukee = sabs2000_milwaukee[(sabs2000_milwaukee['grdlo'].notnull()) | (sabs2000_milwaukee['grdhi'].notnull())]

# clean montgomery county data
sabs2000_montgomery = sabs2000_montgomery_unclean

# make column names lower
sabs2000_montgomery = sabs2000_montgomery.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_montgomery['ccd_id'] = sabs2000_montgomery['ccd_id'].astype(str)
sabs2000_montgomery['ccd_id'] = sabs2000_montgomery['ccd_id'].str.rstrip('.0')

sabs2000_montgomery.loc[sabs2000_montgomery['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_montgomery['ccd_id']
sabs2000_montgomery.loc[sabs2000_montgomery['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_montgomery['ccd_id']
sabs2000_montgomery.loc[sabs2000_montgomery['ccd_id'].str.len() == 2, 'ccd_id'] = '000' + sabs2000_montgomery['ccd_id']
sabs2000_montgomery.loc[sabs2000_montgomery['ccd_id'].str.len() == 1, 'ccd_id'] = '0000' + sabs2000_montgomery['ccd_id']
sabs2000_montgomery['ccd_id'] = sabs2000_montgomery['ccd_id'].replace(['00nan'], [''])

# strip string columns
for column in sabs2000_montgomery.columns:
    if sabs2000_montgomery.dtypes[column] == 'object':
        sabs2000_montgomery[column] = sabs2000_montgomery[column].str.strip()

# add geoleaid for montgomery county schools
sabs2000_montgomery['geoleaid'] = '2400480'
sabs2000_montgomery['ncessch'] = sabs2000_montgomery['geoleaid'] + sabs2000_montgomery['ccd_id'].astype(str)

# merge montgomery sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_montgomery = sabs2000_montgomery.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_montgomery = sabs2000_montgomery[(sabs2000_montgomery['grdlo'].notnull()) | (sabs2000_montgomery['grdhi'].notnull())]

# clean orange county data
sabs2000_orange = sabs2000_orange_unclean

# make column names lower
sabs2000_orange = sabs2000_orange.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_orange['ccd_id'] = sabs2000_orange['ccd_id'].astype(str)
sabs2000_orange['ccd_id'] = sabs2000_orange['ccd_id'].str.rstrip('.0')

sabs2000_orange.loc[sabs2000_orange['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_orange['ccd_id']
sabs2000_orange.loc[sabs2000_orange['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_orange['ccd_id']
sabs2000_orange.loc[sabs2000_orange['ccd_id'].str.len() == 2, 'ccd_id'] = '000' + sabs2000_orange['ccd_id']
sabs2000_orange['ccd_id'] = sabs2000_orange['ccd_id'].replace(['00nan'], [''])

# strip string columns
for column in sabs2000_orange.columns:
    if sabs2000_orange.dtypes[column] == 'object':
        sabs2000_orange[column] = sabs2000_orange[column].str.strip()

# add geoleaid for orange schools
sabs2000_orange['geoleaid'] = '1201440'
sabs2000_orange['ncessch'] = sabs2000_orange['geoleaid'] + sabs2000_orange['ccd_id'].astype(str)

# merge orange sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_orange = sabs2000_orange.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_orange = sabs2000_orange[(sabs2000_orange['grdlo'].notnull()) | (sabs2000_orange['grdhi'].notnull())]

# clean palm beach data
sabs2000_palmbeach = sabs2000_palmbeach_unclean

# make column names lower
sabs2000_palmbeach = sabs2000_palmbeach.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_palmbeach['ccd_id'] = sabs2000_palmbeach['ccd_id'].astype(str)
sabs2000_palmbeach.loc[sabs2000_palmbeach['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_palmbeach['ccd_id']
sabs2000_palmbeach.loc[sabs2000_palmbeach['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_palmbeach['ccd_id']

# strip string columns
for column in sabs2000_palmbeach.columns:
    if sabs2000_palmbeach.dtypes[column] == 'object':
        sabs2000_palmbeach[column] = sabs2000_palmbeach[column].str.strip()

# add geoleaid for palm beach schools
sabs2000_palmbeach['geoleaid'] = '1201500'
sabs2000_palmbeach['ncessch'] = sabs2000_palmbeach['geoleaid'] + sabs2000_palmbeach['ccd_id'].astype(str)

# merge palm beach sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_palmbeach = sabs2000_palmbeach.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_palmbeach = sabs2000_palmbeach[(sabs2000_palmbeach['grdlo'].notnull()) | (sabs2000_palmbeach['grdhi'].notnull())]

# clean philadelphia data
sabs2000_philadelphia = sabs2000_philadelphia_unclean

# make column names lower
sabs2000_philadelphia = sabs2000_philadelphia.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_philadelphia['ccd_id'] = sabs2000_philadelphia['ccd_id'].astype(str)
sabs2000_philadelphia.loc[sabs2000_philadelphia['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_philadelphia['ccd_id']
sabs2000_philadelphia.loc[sabs2000_philadelphia['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_philadelphia['ccd_id']

# strip string columns
for column in sabs2000_philadelphia.columns:
    if sabs2000_philadelphia.dtypes[column] == 'object':
        sabs2000_philadelphia[column] = sabs2000_philadelphia[column].str.strip()

# add geoleaid for philly schools
sabs2000_philadelphia['geoleaid'] = '4218990'
sabs2000_philadelphia['ncessch'] = sabs2000_philadelphia['geoleaid'] + sabs2000_philadelphia['ccd_id'].astype(str)

# merge philly sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_philadelphia = sabs2000_philadelphia.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_philadelphia = sabs2000_philadelphia[(sabs2000_philadelphia['grdlo'].notnull()) | (sabs2000_philadelphia['grdhi'].notnull())]

# clean pinellas data
sabs2000_pinellas = sabs2000_pinellas_unclean

# make column names lower
sabs2000_pinellas = sabs2000_pinellas.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_pinellas['ccd_id'] = sabs2000_pinellas['ccd_id'].astype(str)
sabs2000_pinellas['ccd_id'] = sabs2000_pinellas['ccd_id'].str.rstrip('.0')

sabs2000_pinellas.loc[sabs2000_pinellas['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_pinellas['ccd_id']
sabs2000_pinellas.loc[sabs2000_pinellas['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_pinellas['ccd_id']
sabs2000_pinellas.loc[sabs2000_pinellas['ccd_id'].str.len() == 2, 'ccd_id'] = '000' + sabs2000_pinellas['ccd_id']
sabs2000_pinellas['ccd_id'] = sabs2000_pinellas['ccd_id'].replace(['00nan'], [''])

# strip string columns
for column in sabs2000_pinellas.columns:
    if sabs2000_pinellas.dtypes[column] == 'object':
        sabs2000_pinellas[column] = sabs2000_pinellas[column].str.strip()

# add geoleaid for pinellas schools
sabs2000_pinellas['geoleaid'] = '1201560'
sabs2000_pinellas['ncessch'] = sabs2000_pinellas['geoleaid'] + sabs2000_pinellas['ccd_id'].astype(str)

# merge pinellas sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_pinellas = sabs2000_pinellas.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_pinellas = sabs2000_pinellas[(sabs2000_pinellas['grdlo'].notnull()) | (sabs2000_pinellas['grdhi'].notnull())]

# clean prince george's county data
sabs2000_princegeorge = sabs2000_princegeorge_unclean

# make column names lower
sabs2000_princegeorge = sabs2000_princegeorge.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_princegeorge['ccd_id'] = sabs2000_princegeorge['ccd_id'].astype(str)
sabs2000_princegeorge.loc[sabs2000_princegeorge['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_princegeorge['ccd_id']
sabs2000_princegeorge.loc[sabs2000_princegeorge['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_princegeorge['ccd_id']


# strip string columns
for column in sabs2000_princegeorge.columns:
    if sabs2000_princegeorge.dtypes[column] == 'object':
        sabs2000_princegeorge[column] = sabs2000_princegeorge[column].str.strip()

# add geoleaid for prince george's schools
sabs2000_princegeorge['geoleaid'] = '2400510'
sabs2000_princegeorge['ncessch'] = sabs2000_princegeorge['geoleaid'] + sabs2000_princegeorge['ccd_id'].astype(str)

# merge prince george's sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_princegeorge = sabs2000_princegeorge.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_princegeorge = sabs2000_princegeorge[(sabs2000_princegeorge['grdlo'].notnull()) | (sabs2000_princegeorge['grdhi'].notnull())]

# clean san diego data
sabs2000_sandiego = sabs2000_sandiego_unclean

# make column names lower
sabs2000_sandiego = sabs2000_sandiego.rename(str.lower, axis='columns')

# fix stripped ccd_ids --> should be 6 characters, so add 0s where they are less
sabs2000_sandiego['ccd_id'] = sabs2000_sandiego['ccd_id'].astype(str)
sabs2000_sandiego['ccd_id'] = sabs2000_sandiego['ccd_id'].str.rstrip('.0')

sabs2000_sandiego.loc[sabs2000_sandiego['ccd_id'].str.len() == 4, 'ccd_id'] = '0' + sabs2000_sandiego['ccd_id']
sabs2000_sandiego.loc[sabs2000_sandiego['ccd_id'].str.len() == 3, 'ccd_id'] = '00' + sabs2000_sandiego['ccd_id']
sabs2000_sandiego['ccd_id'] = sabs2000_sandiego['ccd_id'].replace(['00nan'], [''])

# strip string columns
for column in sabs2000_sandiego.columns:
    if sabs2000_sandiego.dtypes[column] == 'object':
        sabs2000_sandiego[column] = sabs2000_sandiego[column].str.strip()

# add geoleaid for san diego schools
sabs2000_sandiego['geoleaid'] = '0634320'
sabs2000_sandiego['ncessch'] = sabs2000_sandiego['geoleaid'] + sabs2000_sandiego['ccd_id'].astype(str)

# merge san diego sabs dataset with ccd 2000 record of schools to find clark county schools that serve 3rd grade
sabs2000_sandiego = sabs2000_sandiego.merge(ccdschools_2000, on='ncessch', how='left')
sabs2000_sandiego = sabs2000_sandiego[(sabs2000_sandiego['grdlo'].notnull()) | (sabs2000_sandiego['grdhi'].notnull())]

# GET SINGLE STRING OF ALL SCHOOLS/CATCHMENT ZONES TO INCLUDE IN SHAPEFILE FOR 1999 - 2000

sabs_2000 = []

# add baltimore
for ccd_id in list(sabs2000_baltimore['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)
    
# add baltimore county
for ccd_id in list(sabs2000_baltimorecounty['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)
    
# add broward county
for ccd_id in list(sabs2000_broward['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)
    
# add chicago
for ccd_id in list(sabs2000_chicago['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)
    
# add clark county
for ccd_id in list(sabs2000_clark['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)
    
# add dallas
for ccd_id in list(sabs2000_dallas['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)

# add detroit
for ccd_id in list(sabs2000_detroit['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)

# add duval county
for ccd_id in list(sabs2000_duval['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)

# add fairfax county
for ccd_id in list(sabs2000_fairfax['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)

# add hillsborough county
for ccd_id in list(sabs2000_hillsborough['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)

# add houston
for ccd_id in list(sabs2000_houston['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)

# add los angeles
for ccd_id in list(sabs2000_la['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)

# add miami dade
for ccd_id in list(sabs2000_miamidade['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)
    
# add milwaukee
for ccd_id in list(sabs2000_milwaukee['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)

# add montgomery county
for ccd_id in list(sabs2000_montgomery['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)

# add orange county
for ccd_id in list(sabs2000_orange['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)

# add palm beach 
for ccd_id in list(sabs2000_palmbeach['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)

# add philadelphia 
for ccd_id in list(sabs2000_philadelphia['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)

# add pinellas county 
for ccd_id in list(sabs2000_pinellas['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)

# add prince george's county  
for ccd_id in list(sabs2000_princegeorge['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)

# add san diego 
for ccd_id in list(sabs2000_sandiego['ccd_id']):
    if ccd_id not in sabs_2000:
        sabs_2000.append(ccd_id)
    
filter_2000 = ''
for ccd_id in sabs_2000:
    filter_2000 += "CCD_ID = '" + ccd_id + "' OR "
filter_2000 = filter_2000[:-4]


# SY 2010-11

# import excel file of all schools in SABS 2010-11
sabs2011_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 2010-11/SABS:SABINS/Raw/SABS (NCES Website)/SAA1011.xlsx', header=0)

sabs2011 = sabs2011_unclean

# CLEAN SABS 2011 DATASET

# convert variable names to lowercase
sabs2011.columns = sabs2011.columns.str.lower()

# change variables to correct types
sabs2011['ncessch'] = sabs2011['ncessch'].astype('str')
sabs2011['leaid'] = sabs2011['leaid'].astype('str')

# add leading 0 to geolea, leaid, and school ids where it was chopped off from incorrect variable type
sabs2011.loc[sabs2011['ncessch'].str.len() == 11, 'ncessch'] = '0' + sabs2011['ncessch']
sabs2011.loc[sabs2011['leaid'].str.len() == 6, 'leaid'] = '0' + sabs2011['leaid']

# change lowest grade (gslo) and highest grade (gshi) to numeric values
sabs2011['gslo'] = sabs2011['gslo'].replace(['KG', 'PK', 'UG', 'N'], 
                                            ['0', '-1', np.nan, np.nan])

sabs2011['gshi'] = sabs2011['gshi'].replace(['KG', 'PK', 'UG', 'N'], 
                                            ['0', '-1', np.nan, np.nan])

# change gslo and gshi to numeric values
sabs2011['gslo'] = sabs2011['gslo'].astype('int')
sabs2011['gshi'] = sabs2011['gshi'].astype('int')

# keep only schools that serve 3rd grade
sabs2011 = sabs2011[(sabs2011.gslo <= 3) & (sabs2011.gshi >= 3)]

# merge with ccd school list to get info on schools that should be removed from SABS list
sabs2011 = pd.merge(sabs2011, ccdschools_2011_unclean, on='ncessch', how='left')

# filter out virtual schools
sabs2011 = sabs2011[sabs2011.virtual != 1]

# filter out magnet schools
sabs2011 = sabs2011[sabs2011.magnet != 1]

# filter out non-regular schools 
# school types: 1 = regular, 2 = special education, 3 = vocational, 4 = alternative/other, 5 = ?
sabs2011 = sabs2011[(sabs2011.type != 2) & (sabs2011.type != 3) & (sabs2011.type != 4) & (sabs2011.type != 5)] 

# GET SINGLE STRING OF ALL SCHOOLS/CATCHMENT ZONES TO INCLUDE IN SHAPEFILE FOR 2013-14
filter_2011 = ''
for ncessch in list(sabs2011['ncessch']):
    filter_2011 += "ncessch = '" + ncessch + "' OR "
filter_2011 = filter_2011[:-4]

# SY 2013-14
# import excel file of all schools in SABS 2013-14
sabs2014_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 2013-14/SABS/Raw/SABS_1314.xlsx', header=0)
sabs2014 = sabs2014_unclean

# CLEAN SABS 2014 DATASET

# convert variable names to lowercase
sabs2014.columns = sabs2014.columns.str.lower()

# change variables to correct types
sabs2014['ncessch'] = sabs2014['ncessch'].astype('str')
sabs2014['leaid'] = sabs2014['leaid'].astype('str')

# add leading 0 to geolea, leaid, and school ids where it was chopped off from incorrect variable type
sabs2014.loc[sabs2014['ncessch'].str.len() == 11, 'ncessch'] = '0' + sabs2014['ncessch']
sabs2014.loc[sabs2014['leaid'].str.len() == 6, 'leaid'] = '0' + sabs2014['leaid']

# change lowest grade (gslo) and highest grade (gshi) to numeric values
sabs2014['gslo'] = sabs2014['gslo'].replace(['KG', 'PK', 'UG', 'N'], 
                                            ['0', '-1', np.nan, np.nan])

sabs2014['gshi'] = sabs2014['gshi'].replace(['KG', 'PK', 'UG', 'N'], 
                                            ['0', '-1', np.nan, np.nan])

# change gslo and gshi to numeric values
sabs2014['gslo'] = sabs2014['gslo'].astype('float')
sabs2014['gshi'] = sabs2014['gshi'].astype('float')

# keep only schools that serve 3rd grade
sabs2014 = sabs2014[(sabs2014.gslo <= 3) & (sabs2014.gshi >= 3)]

# merge with ccd school list to get info on schools that should be removed from SABS list
sabs2014 = pd.merge(sabs2014, ccdschools_2014_unclean, on='ncessch', how='left')

# remove non-response districts for SABS 2013-14 from 2014 schools list
nonresponse_2014 = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 2013-14/SABS/Raw/District_Nonresponse.xls', header=0)
nonresponse_2014['LEA_ID'] = nonresponse_2014['LEA_ID'].astype('str')
nonresponse_2014.loc[nonresponse_2014['LEA_ID'].str.len() == 6, 'LEA_ID'] = '0' + nonresponse_2014['LEA_ID']

for leaid in list(nonresponse_2014['LEA_ID']):
    sabs2014 = sabs2014[sabs2014.geoleaid != leaid]

# filter out virtual schools
sabs2014 = sabs2014[sabs2014.virtual != 1]

# filter out magnet schools
sabs2014 = sabs2014[sabs2014.magnet != 1]

# filter out non-regular schools 
# school types: 1 = regular, 2 = special education, 3 = vocational, 4 = alternative/other, 5 = ?
sabs2014 = sabs2014[(sabs2014.type != 2) & (sabs2014.type != 3) & (sabs2014.type != 4) & (sabs2014.type != 5)] 

# GET SINGLE STRING OF ALL SCHOOLS/CATCHMENT ZONES TO INCLUDE IN SHAPEFILE FOR 2013-14
filter_2014 = ''
for ncessch in list(sabs2014['ncessch']):
    filter_2014 += "ncessch = '" + ncessch + "' OR "
filter_2014 = filter_2014[:-4]

# SY 2015-16
# import excel file of all schools in SABS 2015-16
sabs2016_unclean = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 2015-16/SABS/Raw/SABS_1516.xlsx', header=0)
sabs2016 = sabs2016_unclean

# CLEAN SABS 2016 DATASET

# convert variable names to lowercase
sabs2016.columns = sabs2016.columns.str.lower()

# change variables to correct types
sabs2016['ncessch'] = sabs2016['ncessch'].astype('str')
sabs2016['leaid'] = sabs2016['leaid'].astype('str')

# add leading 0 to geolea, leaid, and school ids where it was chopped off from incorrect variable type
sabs2016.loc[sabs2016['ncessch'].str.len() == 11, 'ncessch'] = '0' + sabs2016['ncessch']
sabs2016.loc[sabs2016['leaid'].str.len() == 6, 'leaid'] = '0' + sabs2016['leaid']

# change lowest grade (gslo) and highest grade (gshi) to numeric values
sabs2016['gslo'] = sabs2016['gslo'].replace(['KG', 'PK', 'UG', 'N'], 
                                            ['0', '-1', np.nan, np.nan])

sabs2016['gshi'] = sabs2016['gshi'].replace(['KG', 'PK', 'UG', 'N'], 
                                            ['0', '-1', np.nan, np.nan])

# change gslo and gshi to numeric values
sabs2016['gslo'] = sabs2016['gslo'].astype('float')
sabs2016['gshi'] = sabs2016['gshi'].astype('float')

# keep only schools that serve 3rd grade
sabs2016 = sabs2016[(sabs2016.gslo <= 3) & (sabs2016.gshi >= 3)]

# merge with ccd school list to get info on schools that should be removed from SABS list
sabs2016 = pd.merge(sabs2016, ccdschools_2016_unclean, on='ncessch', how='left')

# remove non-response districts for SABS 2015-16 from 2016 schools list
nonresponse_2016 = pd.read_excel('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 2015-16/SABS/Raw/District_Nonresponse.xls', header=0)
nonresponse_2016['Lea_Id'] = nonresponse_2016['Lea_Id'].astype('str')
nonresponse_2016.loc[nonresponse_2016['Lea_Id'].str.len() == 6, 'Lea_Id'] = '0' + nonresponse_2016['Lea_Id']

for leaid in list(nonresponse_2016['Lea_Id']):
    sabs2016 = sabs2016[sabs2016.geoleaid != leaid]


# filter out virtual schools
sabs2016 = sabs2016[sabs2016.virtual != 1]

# filter out magnet schools
sabs2016 = sabs2016[sabs2016.magnet != 1]

# filter out non-regular schools 
# school types: 1 = regular, 2 = special education, 3 = vocational, 4 = alternative/other, 5 = ?
sabs2016 = sabs2016[(sabs2016.type != 2) & (sabs2016.type != 3) & (sabs2016.type != 4) & (sabs2016.type != 5)] 

# GET SINGLE STRING OF ALL SCHOOLS/CATCHMENT ZONES TO INCLUDE IN SHAPEFILE FOR 2015-16
filter_2016 = ''
for ncessch in list(sabs2016['ncessch']):
    filter_2016 += "ncessch = '" + ncessch + "' OR "
filter_2016 = filter_2016[:-4]



