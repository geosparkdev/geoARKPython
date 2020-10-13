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

MONGO_HOST = "192.168.1.202"
MONGO_USER = "tiffyson"
MONGO_PASS = "Hented123!"


server = SSHTunnelForwarder(
    MONGO_HOST,
    ssh_username=MONGO_USER,
    ssh_password=MONGO_PASS,
    remote_bind_address=('127.0.0.1', 27017)
)


client = pymongo.MongoClient('192.168.1.202')


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