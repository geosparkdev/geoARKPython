from sshtunnel import SSHTunnelForwarder
import pymongo
import json


def connectToDb():

    MONGO_HOST = "192.168.1.202"
    MONGO_USER = "tiffyson"
    MONGO_PASS = "Hented123!"

    server = SSHTunnelForwarder(
        MONGO_HOST,
        ssh_username=MONGO_USER,
        ssh_password=MONGO_PASS,
        remote_bind_address=('127.0.0.1', 27017)
    )

    server.start()
    client = pymongo.MongoClient('192.168.1.202') 

    return client
