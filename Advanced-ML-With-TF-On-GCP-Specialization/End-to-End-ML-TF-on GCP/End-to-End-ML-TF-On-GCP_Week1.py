
# starting query
import google.datalab.bigquery as bq
query = """
select
    weight_pounds
    ,is_male
    ,mother_age
    ,plurality
    ,gestation_weeks
    ,abs(farm_fingerprint(concat(cast(year as string), cast(month as string)))) as hashmonth
from
    publicdata.samples.natality
where
    year > 2000
"""

# lab task 1
# create sampling query with about 12500 rows
sampling_query = """
select * from (
select
    weight_pounds
    ,is_male
    ,mother_age
    ,plurality
    ,gestation_weeks
    ,abs(farm_fingerprint(concat(cast(year as string), cast(month as string)))) as hashmonth
from
    publicdata.samples.natality
where
    year > 2000
    ) where mod(abs(hashmonth), 10) < 8 and rand() 0.0004
"""

# run sampling query (limit to 100 rows)
df = bq.Query(sampling_query).execute().result().to_dataframe()
df.head()

# lab task 2
# remove nulls
sampling_query = """
select * from (
select
    weight_pounds
    ,is_male
    ,mother_age
    ,plurality
    ,gestation_weeks
    ,abs(farm_fingerprint(concat(cast(year as string), cast(month as string)))) as hashmonth
from
    publicdata.samples.natality
where
    year > 2000
    and weight_pounds is not null
    and is_male is not null
    and mother_age is not null
    and plurality is not null
    and gestation_weeks is not null
    ) where mod(abs(hashmonth), 10) < 8 and rand() 0.0004
"""

# simulate lack of ultrasound (is_male column would be unknown)
# copy original df
import copy
df2 = copy.deepcopy(df)

# set is_male to be unknown
df2['is_male'] = 'Unknown'
df2.head()

# change plurality column to be a string
df2['plurality'] = df2['plurality'].loc([df2['plurality'] == 1]) = 'Single'
df2['plurality'] = df2['plurality'].loc([df2['plurality'] != 1]) = 'Multiple'
