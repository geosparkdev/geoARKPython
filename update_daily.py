#  ---------------
#   I M P O R T S 
#  ---------------
from sshtunnel import SSHTunnelForwarder
import pymongo
import pprint
import pandas as pd
import seaborn as sns
from datetime import datetime
import datetime as dt
import numpy as np
import uuid
import json

#  ------------------
#    S E T T I N G S
#  ------------------

pd.set_option('max_colwidth', 100)


#  ----------------
#   M O N G O D B 
#  ----------------


MONGO_HOST = "128.206.117.150"
MONGO_USER = "haithcoatt"
MONGO_PASS = "Ke11ieJean"

server = SSHTunnelForwarder(
    MONGO_HOST,
    ssh_username=MONGO_USER,
    ssh_password=MONGO_PASS,
    remote_bind_address=('127.0.0.1', 27017)
)

server.start()
client = pymongo.MongoClient('127.0.0.1', server.local_bind_port) # server.local_bind_port is assigned local port




#  ----------------
#  USA DAILY COUNTY COVID CASES AND DEATHS
#  ----------------


db = client.metadata


auto_attr= pd.DataFrame(db.auto_attr.find())


metadata_daily= pd.DataFrame(db.metadata.find({"update_frequency":"daily"}))

runs = pd.DataFrame (columns = ['dataset_id','dt_run_start','dt_run_end','status','messages'])

for dataset_id in metadata_daily.dataset_id:
    dt_run_start=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    #Remove old data from bigdata collection
    db.bigdata.remove({"dataset_id":dataset_id})
    
    ## Load new data 
    data=pd.read_csv(metadata_daily.loc[metadata_daily.dataset_id==dataset_id].source_url.values[0])
    
    attr_info=pd.DataFrame(db.auto_attr.find({"dataset_id":dataset_id}))
    
    attributes = pd.DataFrame(data.columns,columns=['attr_orig'])
  
    ## build attributes table
    attributes['attr_desc']=attr_info.attr_desc.values[0]+" "+attributes.attr_orig
    attributes['attr_id']=attr_info.attr_id.values[0]
    attributes['start_date']=attr_info.start_date.values[0]
    attributes['end_date']=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    attributes['attr_dtype']=attr_info.attr_dtype.values[0]
    attributes['iso_key']=attr_info.iso_key.values[0]
    attributes['iso_key_add']=attr_info.iso_key_add.values[0]
    attributes['scale']=attr_info.scale.values[0]
    attributes['positional_accuracy']=attr_info.positional_accuracy.values[0]
    attributes['spatial_rep']=attr_info.spatial_rep.values[0]
    attributes['datum']=attr_info.datum.values[0]
    attributes['coordinate_system']=attr_info.coordinate_system.values[0]
    attributes['entity_type']=attr_info.entity_type.values[0]
    
 
    attributes['tags']=attr_info.tags.values[0][0]+'_'+attr_info.tags.values[0][1]
    attributes['tags']=attributes['tags'].str.split('_')

    ##correct for FIPS/location fields
    attributes.loc[np.arange(0,attr_info.data_start.values[0]),'attr_desc'] = attributes.attr_orig
    attributes.loc[np.arange(0,attr_info.data_start.values[0]),'attr_dtype'] = attr_info.attr_dtype_loc.values[0]
    attributes.loc[np.arange(0,attr_info.data_start.values[0]),'iso_key'] = attr_info.attr_dtype_loc.values[0]
    attributes.loc[np.arange(0,attr_info.data_start.values[0]),'iso_key_add'] = attr_info.iso_key_add_loc.values[0]
    attributes.loc[np.arange(0,attr_info.data_start.values[0]),'attr_id'] = attr_info.attr_id_loc.values[0]
    


    expected=['start_date',
            'end_date',
            'iso_key',
            'iso_key_add',
            'attr_id',
            'attr_orig',
            'attr_desc',
            'attr_dtype',
            'scale',
            'positional_accuracy',
            'spatial_rep',
            'datum',
            'coordinate_system',
            'entity_type',
             'tags']


    violation=0

    for i in attributes.columns:
        if i not in expected:
            violation+=1

    if violation==0:
        attributes.insert(loc=0, column='dataset_id', value=dataset_id)

        attributes.insert(loc=1, column='attr_label', value=np.nan)

        for i,x in zip(attributes.attr_orig,range(1,len(attributes.attr_orig)+1)):
            attributes.loc[attributes.attr_orig==i, 'attr_label'] = dataset_id+"_"+str(x).zfill(2)

        # Add attribute key object to database-- if field "Attributes" already exists, will overwrite data stored there
            db.metadata.update_one(
                {
                    "dataset_id":dataset_id
                },
                {
                    "$set":{
                            "attributes":attributes.to_dict('record')
                            }
                 },
                upsert=False,
                array_filters=None
            )

        result="Correct. Attribute Key added"
    else:
        result="csv file does not match template, please check names"

    #update upload time in metadata table
    db.metadata.update_one({"dataset_id":dataset_id },{"$set":{"upload_date":datetime.today().strftime('%Y-%m-%d %H:%M:%S'),"upload_user":"tcy8v6"}})
    
    #get dataset information and attribute look-up table
    dataset=pd.DataFrame(db.metadata.find({"dataset_id":dataset_id}))
    
    attributes_lookup=pd.DataFrame(dataset.attributes[0])
    
    for i,x,s in attributes_lookup[['attr_label','attr_orig','attr_dtype']].itertuples(index=False):
        data.rename(columns={x:i}, inplace=True )
        if s=='FL_PER':
            data[i] = data[i].str.rstrip('%').astype('float') / 100.0
        elif s=='CHAR_L':
            data[i] = data[i].map(lambda x: x.lstrip('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz')).astype(int)
        elif s=='DATETIME':
            data[i] = pd.to_datetime(data[i])


    data.insert(loc=0, column='dataset_id', value=dataset_id)
    
    db.bigdata.insert_many(data.to_dict('records'))  
    print(dataset_id)
    status="did not test"
    test=pd.DataFrame(db.bigdata.find({"dataset_id":dataset_id},{"_id":0}))

    if test.empty:
        status='failed'
    else:
        status='passed'
    
    dt_run_end=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    
    
    runs = runs.append({'dataset_id':dataset_id,
                    'dt_run_start':dt_run_start,
                    'dt_run_end':dt_run_end,
                    'status':status,
                    'messages':result}, ignore_index=True)
    

runs["dt_run_start"] = pd.to_datetime(runs["dt_run_start"])
runs["dt_run_end"] = pd.to_datetime(runs["dt_run_end"])
db.daily_runs.insert(runs.to_dict('records'))



#  ----------------
# COVID CASES AND DEATHS -- MISSOURI COVID DASHBOARD
#  ----------------

db = client.metadata


metadata_cases=pd.DataFrame(db.metadata.find({'dataset_id':'4fd71eac_01_daily'}))
metadata_deaths=pd.DataFrame(db.metadata.find({'dataset_id':'4fd71eac_02_daily'}))

# get attributes look-up table from metadata table
attributes_cases=pd.json_normalize(metadata_cases["attributes"][0])
attributes_deaths=pd.json_normalize(metadata_deaths["attributes"][0])

MO_cases = pd.DataFrame(db.bigdata.find({"dataset_id":'4fd71eac_01_daily','4fd71eac_01_daily_03':'MO'},{'_id':0}))


# get data and grab original label (dates)
columns = ['dataset_id'] + attributes_cases['attr_orig'].to_list()
fixed_columns=columns[:5]
dates=columns[5:]
dates_fixed = ['{}/{}/{}'.format(m,d,y) for y, m, d in map(lambda x: str(x).split('-'), dates)]
dates_fixed=[s.lstrip("0") for s in dates_fixed]
dates_fixed = [item.replace('/0','/') for item in dates_fixed]
for i in dates_fixed:
    fixed_columns.append(i)
    
MO_cases.columns = fixed_columns



MO_deaths = pd.DataFrame(db.bigdata.find({"dataset_id":'4fd71eac_02_daily','4fd71eac_02_daily_03':'MO'},{'_id':0}))
columns = ['dataset_id'] + attributes_deaths['attr_orig'].to_list()
fixed_columns=columns[:5]
dates=columns[5:]
dates_fixed = ['{}/{}/{}'.format(m,d,y) for y, m, d in map(lambda x: str(x).split('-'), dates)]
dates_fixed=[s.lstrip("0") for s in dates_fixed]
dates_fixed = [item.replace('/0','/') for item in dates_fixed]
for i in dates_fixed:
    fixed_columns.append(i)
    
MO_deaths.columns = fixed_columns


MO_cases=MO_cases.drop(columns={'dataset_id','County Name','State','StateFIPS'})
MO_cases=MO_cases.add_prefix("covid_cases_")
MO_cases=MO_cases.rename(columns={'covid_cases_countyFIPS':'fips'})

MO_deaths=MO_deaths.drop(columns={'dataset_id','County Name','State','StateFIPS'})
MO_deaths=MO_deaths.add_prefix("covid_deaths_")
MO_deaths=MO_deaths.rename(columns={'covid_deaths_countyFIPS':'fips'})


together=MO_deaths.merge(MO_cases, on='fips',how='left')

final=together.loc[together.fips!=0]

db = client.covid_dash
db.modeling_covid.remove({})

db.modeling_covid.insert_many(final.to_dict('record'))


#  ----------------
# WEEKLY CASES/ DEATHS-- MISSOURI COVID DASHBOARD
#  ----------------



db = client.metadata
metadata_cases=pd.DataFrame(db.metadata.find({'dataset_id':'4fd71eac_01_daily'}))
metadata_deaths=pd.DataFrame(db.metadata.find({'dataset_id':'4fd71eac_02_daily'}))

# get attributes look-up table from metadata table
attributes_cases=pd.json_normalize(metadata_cases["attributes"][0])
attributes_deaths=pd.json_normalize(metadata_deaths["attributes"][0])

# get data and grab original label (dates)
MO_cases = pd.DataFrame(db.bigdata.find({"dataset_id":'4fd71eac_01_daily','4fd71eac_01_daily_03':'MO'},{'_id':0}))
columns = ['dataset_id'] + attributes_cases['attr_orig'].to_list()
MO_cases.columns = columns

MO_deaths = pd.DataFrame(db.bigdata.find({"dataset_id":'4fd71eac_02_daily','4fd71eac_02_daily_03':'MO'},{'_id':0}))
columns = ['dataset_id'] + attributes_deaths['attr_orig'].to_list()
MO_deaths.columns = columns

dates=columns[5:]
MO_cases['total_cases']=MO_cases[dates].sum(axis=1)
MO_deaths['total_deaths']=MO_deaths[dates].sum(axis=1)

# start building bar data
bar_data=MO_cases[['countyFIPS','County Name','total_cases']].merge(MO_deaths[['countyFIPS','total_deaths']], on='countyFIPS', how='left')
bar_data=bar_data.loc[bar_data.countyFIPS!=0]

db = client.covid_dash
db.covid_totals.remove({})
db.covid_totals.insert_many(bar_data.to_dict('record'))







#  ----------------
# RISK TOTALS -- MISSOURI COVID DASHBOARD
#  ----------------

## Categories main data

risk_totals=pd.DataFrame(db.riskfactor_totals.find({},{'_id':0,'normalized_0_5':0}))
covid_totals=pd.DataFrame(db.covid_totals.find({},{'_id':0}))
filters=pd.DataFrame(list(db.filters.find({},{"_id":0})))

risk_totals=risk_totals.rename(columns={'cnty_fips':'countyFIPS'})
filters=filters.rename(columns={'cnty_fips':'countyFIPS'})


totals=risk_totals.merge(covid_totals, on='countyFIPS', how='left')
totals=totals.merge(filters, on='countyFIPS', how='left')
totals=totals.rename(columns={'risk_total':'total_risk'})

db = client.covid_dash
db.categories_totals.remove({})
db.categories_totals.insert_many(totals.to_dict('record'))