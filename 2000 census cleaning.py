# import necessary libraries
import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'


# import 1999-2000 census demographics data
demographics00_unclean = pd.read_csv('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Census Data/Raw/nhgis0028_ds147_2000_block.csv', header=0, low_memory=False)

print('imported successfully!')

demographics00 = demographics00_unclean

# clean census demographics data
demographics00 = demographics00[['GISJOIN', 'YEAR', 'STATEA', 'NAME', 'BLOCKA', 'FYM002', 'FYM025', 'FYO002', 'FYO025', 
                                'FYO048', 'FYO071', 'FYO094', 'FYO117', 'FYO140', 'FYO163', 'FYO186', 'FYO209', 
                                'FYO232', 'FYO255', 'FYO278', 'FYO301', 'FYQ002', 'FYQ025']]

# rename variables
demographics00 = demographics00.rename(columns={'FYO002' : 'white_male', 
                                                'FYO025' : 'white_female',
                                                'FYO048' : 'black_male',
                                                'FYO071' : 'black_female',
                                                'FYO094' : 'native_male',
                                                'FYO117' : 'native_female',
                                                'FYO140' : 'asian_male',
                                                'FYO163' : 'asian_female',
                                                'FYO186' : 'hawaiianpacific_male',
                                                'FYO209' : 'hawaiianpacific_female',
                                                'FYO232' : 'other_male',
                                                'FYO255' : 'other_female',
                                                'FYO278' : 'multiracial_male',
                                                'FYO301' : 'multiracial_female',
                                                'FYQ002' : 'latino_male',
                                                'FYQ025' : 'latino_female',
                                                'FYM002' : 'male_5_9',
                                                'FYM025' : 'female_5_9'})

# convert variable names to lowercase
demographics00.columns = demographics00.columns.str.lower()

# get total count of each race by adding together the male and female counts of each race
demographics00['white_5_9'] = demographics00['white_male'] + demographics00['white_female']
demographics00['black_5_9'] = demographics00['black_male'] + demographics00['black_female']
demographics00['native_5_9'] = demographics00['native_male'] + demographics00['native_female']
demographics00['asian_5_9'] = demographics00['asian_male'] + demographics00['asian_female']
demographics00['hawaiianpacific_5_9'] = demographics00['hawaiianpacific_male'] + demographics00['hawaiianpacific_female']
demographics00['other_5_9'] = demographics00['other_male'] + demographics00['other_female']
demographics00['multiracial_5_9'] = demographics00['multiracial_male'] + demographics00['multiracial_female']
demographics00['hispanic_latino_5_9'] = demographics00['latino_male'] + demographics00['latino_female']
demographics00['total_5_9'] = demographics00['male_5_9'] + demographics00['female_5_9']

# drop variables containing racial counts by sex
demographics00 = demographics00[['gisjoin', 
                                 'year',
                                 'statea',
                                 'name',
                                 'blocka',
                                 'white_5_9',
                                 'black_5_9',
                                 'native_5_9',
                                 'asian_5_9',
                                 'hawaiianpacific_5_9',
                                 'other_5_9',
                                 'multiracial_5_9',
                                 'hispanic_latino_5_9',
                                 'total_5_9']]
                                 
# export clean csv to documents
demographics10.to_csv('/Users/lacm/Documents/Stanford/*Capstone Research/Data/SY 1999-2000/Census Data/Clean/demographics2000.csv')
