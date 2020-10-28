#  ---------------
#   I M P O R T S
#  ---------------

import json
import flask
from flask import jsonify, request
from flask_cors import CORS
from sshtunnel import SSHTunnelForwarder
import pymongo
import pprint
import uuid as uuid
from io import StringIO
import base64

from datetime import datetime
import pandas as pd
import seaborn as sns
import numpy as np

from urllib.request import urlopen 
import plotly.express as px
import ssl

MONGO_HOST = "128.206.117.150"
MONGO_USER = "haithcoatt"
MONGO_PASS = "Ke11ieJean"

server = SSHTunnelForwarder(
    MONGO_HOST,
    ssh_username=MONGO_USER,
    ssh_password=MONGO_PASS,
    remote_bind_address=('127.0.0.1', 27017)
)

client = pymongo.MongoClient('127.0.0.1', server.local_bind_port) # server.local_bind_port is assigned local port


def create_app(test_config=None):
    app = flask.Flask(__name__)
    CORS(app)


#########################################################################


    @app.route('/dbMap', methods=['GET'])
    def getMap():

        sslContext = ssl.SSLContext()
        with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json',
            context=sslContext) as response:
            counties = json.load(response)
        covid_cases=pd.read_csv('https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_confirmed_usafacts.csv')
        MO_covid=covid_cases.loc[covid_cases.stateFIPS==29].iloc[1:]
        MO_covid=MO_covid[['countyFIPS','County Name','10/8/20']]
        MO_covid=MO_covid.rename(columns={'County Name':'county','10/8/20':'deaths'})
        state_FIPS='29'
        chosen=[]
        for i in range(0,len(counties.get("features"))):
            if counties.get("features")[i].get("properties").get("STATE")==state_FIPS:
                temp=counties.get("features")[i]
                temp.get("properties")["covid_deaths"]=str(MO_covid.loc[MO_covid.countyFIPS==int(counties.get("features")[i].get("properties").get("STATE")+counties.get("features")[i].get("properties").get("COUNTY"))].deaths.values[0])
                chosen.append(temp)
        new_dict={'type':'FeatureCollection','features':chosen}
        new_dict
        return jsonify(new_dict)




    @app.route('/buildObject', methods=['GET'])
    def buildObject():
        #get updated covid cases
        covid_cases=pd.read_csv("https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_confirmed_usafacts.csv")
        MO_covid_cases=covid_cases.loc[covid_cases.State=='MO']
        end=len(MO_covid_cases.columns)
        keys=MO_covid_cases.columns[4:end]

        #get geoJSON data
        sslContext = ssl.SSLContext()
        with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json',
            context=sslContext) as response:
            counties = json.load(response)


        state_FIPS='29'
        chosen=[]
        temp=[]
        for i in range(0,len(counties.get("features"))):
            if counties.get("features")[i].get("properties").get("STATE")==state_FIPS:
                temp=counties.get("features")[i]
                for x in keys:  
                    temp.get("properties")[x]=str(MO_covid_cases.loc[MO_covid_cases.countyFIPS==int(counties.get("features")[i].get("properties").get("STATE")+counties.get("features")[i].get("properties").get("COUNTY"))][x].values[0])
                chosen.append(temp)



        new_dict={'type':'FeatureCollection','features':chosen}

        legend=pd.DataFrame(MO_covid_cases.max()[4:]).reset_index().rename(columns={'index':'keys',0:'max'})
        legend=legend.merge(pd.DataFrame(MO_covid_cases.min()[4:]).reset_index().rename(columns={'index':'keys',0:'min'}),on='keys',how='left')
        legend['max']=legend['max'].astype(str)
        legend['min']=legend['min'].astype(str)
        legend['display']="COVID 19 Cases"
        legend=legend.to_dict('records')

        


        return jsonify([new_dict,legend])


    @app.route('/getDataCat', methods=['POST'])
    def getDataCat():

        server.start()
        db = client.metadata
        requestData=json.loads(request.data)


        ## search metadata table for attributes that have been tagged with category and subcategory keywords
        metadata_search=pd.DataFrame(db.metadata.aggregate([
                {
                "$project": 
                        {
                        "attributes.tags":1,
                            "_id":0,
                            "dataset_id":1,
                            "originator_id":1,
                            "loc_id":1,
                            "attributes.dataset_id":1,
                            "attributes.entity_type":1,
                            "attributes.attr_label":1,
                            "attributes.attr_desc":1,
                            "attributes.start_date":1,
                            "attributes.end_date":1

                        },
                    
            },
            {
                "$unwind":"$attributes"
            },
            {
                "$match":{
                    "$and":[
                        {"attributes.tags":requestData[0]},
                        {"attributes.tags":requestData[1]}
                    ]

                
                }
            },
            {
                "$lookup":
                {
                    "from":"originators",
                    "localField":"originator_id",
                    "foreignField":"originator_id",
                    "as":"originator"
                }
            },
            {
                "$unwind":"$originator"
            }
        ]))
    
       
        ## pull out attributes and originator data
        attributes=pd.json_normalize(metadata_search["attributes"])

        originators=pd.json_normalize(metadata_search["originator"])
        originators=originators.drop(columns="_id")

        ## drop attributes and originator fields from metadata search table
        metadata_search=metadata_search.drop(columns={"attributes","originator"})     

        ## create completed metadata table
        metadata=pd.DataFrame()
        metadata=metadata_search.drop_duplicates().merge(attributes,on='dataset_id',how='left')
        metadata=metadata.merge(originators[['originator_id','originator_name']].drop_duplicates(),on='originator_id',how='left')  

        ## find max and min values and add to metadata
        temp_df=pd.DataFrame()
        for index,row in metadata[['dataset_id','loc_id','attr_label']].iterrows():
            meta_temp=pd.DataFrame(db.bigdata.find({"dataset_id":row.dataset_id},{row.loc_id:1,row.attr_label:1,"_id":0}))
            upper=meta_temp[row.attr_label].max()
            lower=meta_temp[row.attr_label].min()
            temp_df=temp_df.append({"attr_label":row.attr_label,
                                "min":lower,
                                "max":upper}, ignore_index=True)
        metadata=metadata.merge(temp_df, on='attr_label',how='left')

        ## pull data from big data table 
        data=[]

        for index,row in metadata_comp[['dataset_id','loc_id','attr_label']].iterrows():
            data_dict=pd.DataFrame(db.bigdata.find({"dataset_id":row['dataset_id']},{row['loc_id']:1,row['attr_label']:1,"_id":0})).to_dict()
            data.append(data_dict.copy())



        ## get geoJSON object for counties from database
        counties=pd.DataFrame(list(db.geoJSON_county.find({},{"_id":0})))

        counties=counties.to_dict('records')

        features=[]
        for i in range(0,len(counties)):
            features.append(counties[i]['features'])

        ## pull out just features from original geoJSON object
        just_counties=pd.DataFrame(features)


        ## create table with just Missouri counties
        MO_counties=just_counties[just_counties.id.str.startswith('29')]
        MO_counties=MO_counties.reset_index(drop=True)


        ## pull out properties object from MO_counties table and create fips field to join data to properties table
        properties=pd.json_normalize(MO_counties["properties"])
        properties['fips']=(properties.STATE+properties.COUNTY).astype(int)


        ## add data to properties tables per county
        for i in range(0,len(data)):
            properties=properties.merge(pd.DataFrame(data[i]),how='left',left_on=['fips'],right_on=[list(data[i].keys())[0]])
            properties=properties.drop(columns=list(data[i].keys())[0])

        ## create new properties table
        properties=properties.reset_index(drop=True)
        updated_attr=pd.DataFrame({"properties":properties.to_dict('records')})

        #create final geoJSON object
        geoJSON={"type":"FeatureCollection","features":back_together.to_dict('records')}

        final=[geoJSON,metadata.to_dict('records')]

        server.stop()

        return jsonify(final)


#########################################################################
##########              U P L O A D     T O O L               ###########
#########################################################################

    @app.route('/getoriginator', methods=['GET'])
    def getOriginator():
 
        server.start()

    
        db = client.metadata

        originators=list(db.originators.find({},{"_id":0}))

        server.stop()

        return jsonify(originators)



    @app.route('/addoriginator', methods=['POST'])
    def addOriginator():
        requestData=json.loads(request.data)
        server.start()

        db = client.metadata

        originator={
            "originator_name":requestData.get('originator_name'),
            "originator_id":str(uuid.uuid4())[:8],
            "num_datasets":0,
        }

        db.originators.insert_one(originator)

        originator=list(db.originators.find({"originator_name":str(requestData.get('originator_name'))},{"_id":0,"originator_name":0}))
        originator_id=originator[0]["originator_id"]

        server.stop()

        return jsonify(originator_id)


    @app.route('/getdatasets', methods=['GET'])
    def getDatasets():
        orig_id=request.args['orig_id']
        server.start()

    
        db = client.metadata

        datasets=list(db.metadata.find({"originator_id":orig_id,"current_version":True},{"dataset_id":1,"dataset_name":1,"_id":0,"upload_date":1}))

        server.stop()

        return jsonify(datasets)


    @app.route('/getdatasetinfo', methods=['GET'])
    def getDatasetinfo():
        dataset_id=request.args['dataset_id']
        server.start()

    
        db = client.metadata

        dataset_info=list(db.metadata.find({'dataset_id':dataset_id},{"_id":0,"attributes":0}))

        server.stop()

        return jsonify(dataset_info)

    @app.route('/adddataset', methods=['POST'])
    def addDataset():
        requestData=json.loads(request.data)
        server.start()

        db = client.metadata

        dataset_id=requestData.get('originator_id')+"_"+str(db.metadata.find({"originator_id":requestData.get('originator_id')}).count()).zfill(2)+"_01"

        dataset_info={"dataset_id":dataset_id,
                        "originator_id":requestData.get('originator_id'),
                        "dataset_name":requestData.get('dataset_name'),
                        "use_constraint":requestData.get('use_constraint'),
                        "access_constraint":requestData.get('access_constraint'),
                        "security_class":requestData.get('security_class'),
                        "datausage_agreement":requestData.get('datausage_agreement'),
                        "native_format":requestData.get('native_format'),
                        "date_created":datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                        "createdby":requestData.get('createdby'),
                        "upload_user":None,
                        "upload_date":None,
                        "current_version":True,
                        "source_url":requestData.get('source_url'),
                        "update_frequency":requestData.get('update_frequency')
             }

        db.metadata.insert_one(dataset_info)

        #increment dataset count
            
        db.originators.update_one({"originator_id":str(requestData.get('originator_id')) },{"$inc":{"num_datasets":1}})

        server.stop()

        return jsonify(dataset_id)


    @app.route('/uploadattributes', methods=['POST'])
    def uploadAttributes():
        requestData=json.loads(request.data)
        server.start()
        db = client.metadata

        data=requestData.get('attributescsv')


        newAttr = data[21:] + "==="
        file_contents_bytes = base64.b64decode(newAttr)
        file_content = file_contents_bytes.decode("utf-8")
        StringData = StringIO(file_content) 

        attributes = pd.read_csv(StringData, sep =",") 


        # Check to see that csv file matches template
        expected=['start_date',
                'end_date',
                'iso_key',
                'iso_key_add',
                'attr_orig',
                'attr_desc',
                'attr_dtype',
                'scale',
                'positional_accuracy',
                'spatial_rep',
                'datum',
                'coordinate_system',
                'entity_type']


        violation=0

        for i in attributes.columns:
            if i not in expected:
                violation+=1

        dataset_id=requestData.get('dataset_id')
                
        if violation==0:
            attributes.insert(loc=0, column='originator_id', value=dataset_id)

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

        server.stop()

        return jsonify(result)


    @app.route('/uploaddata', methods=['POST'])
    def uploadData():
        requestData=json.loads(request.data)
        server.start()
        db = client.metadata

        data=requestData.get('datacsv')


        newData = data[21:] + "==="
        file_contents_bytes = base64.b64decode(newData)
        file_content = file_contents_bytes.decode("utf-8")
        StringData = StringIO(file_content) 

        data = pd.read_csv(StringData, sep =",") 

        ## grab dataset and attribute look up table
        dataset=pd.DataFrame(db.metadata.find({"dataset_id":requestData.get('dataset_id')}))    
        attributes=pd.DataFrame(list(dataset.attributes[0]))

        for i,x,s in attributes[['attr_label','attr_orig','attr_dtype']].itertuples(index=False):
              data.rename(columns={x:i}, inplace=True )
              if s=='FL_PER':
                data[i] = data[i].str.rstrip('%').astype('float') / 100.0
              elif s=='CHAR_L':
                data[i] = data[i].map(lambda x: x.lstrip('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz')).astype(int)
              elif s=='DATETIME':
                data[i] = pd.to_datetime(data[i])

        data.insert(loc=0, column='dataset_id', value=requestData.get('dataset_id'))

        db.bigdata.insert_many(data.to_dict('records'))  

        server.stop()
        return jsonify("added")



    return app