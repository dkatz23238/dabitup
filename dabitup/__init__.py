import yaml
import os
import glob
import pprint
import argparse
import pandas as pd
import pymongo
import json
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection
import docker
# import psycopg2
# import pg8000
# import docker
def make_docker_config(DATABASE, DATA_SOURCE, USERNAME, PASSWORD, SERVICE_NAME):
    if DATABASE.lower() == "sql":
        image = "sameersbn/postgresql:10-1"
        ports = '5432:5432'
        env_dict = {
            'DB_PASS': PASSWORD,
        'DB_USER': USERNAME,
            'DB_NAME': "DB"
        }
        
    elif DATABASE.lower() == "mongo":
        image="bitnami/mongodb:latest"
        ports = '27017:27017'
        env_dict = {
            'MONGODB_USERNAME':USERNAME,
            'MONGODB_PASSWORD':PASSWORD,
            'MONGODB_DATABASE':"DB"
            }
    
    config = {
        'version': '3.1',
        'services': {
            SERVICE_NAME : {
                'image': image,
                'restart': 'always',
                'environment': env_dict,
                'ports': [ports] }
                }
            }
    return config

def insert_tables_to_SQL(DATA_SOURCE, USERNAME, PASSWORD, FFORMAT=".csv"):
    assert FFORMAT[0] == "."
    engine = create_engine(
        "postgres://%s:%s@localhost:5432/DB" % (USERNAME, PASSWORD),
        encoding="latin-1"
    )
    assert type(engine.connect()) == Connection

    files = glob.glob(os.path.join(DATA_SOURCE, "*%s"% FFORMAT))
    for f in files:
        print("Reading Table: %s" % f)
        df = pd.read_csv(f, low_memory=True)

        if df.index.name == "":
            df.index.name = ""
        
        print("Pushing Table: %s to DB" % f)
        table_name = f.split("/")[-1].replace(FFORMAT, "").lower()

        df.to_sql(
            name=table_name,
            con=engine,
            chunksize=500,
            index=True)

        print("Table INSERT complete")

def insert_records_to_mongo(DATA_SOURCE, USERNAME, PASSWORD, FFORMAT=".json"):
    assert FFORMAT[0] == "."
    db_str = "mongodb://%s:%s@localhost:27017/DB" % (USERNAME, PASSWORD)
    client = pymongo.MongoClient(db_str)
    collection = client["DB"]["records"]
    files = glob.glob(os.path.join(DATA_SOURCE, "*%s" %FFORMAT ))
    for f in files:
        with open(f, "r") as data:
            j = json.loads(data.read())
            if type(j) == list:
                for records in j:
                    collection.insert_one(records)
            elif type(j) == dict:
                collection.insert_one(j)

def return_files(DATA_SOURCE, FFORMAT):
    files = glob.glob(os.path.join(DATA_SOURCE, "*%s" % FFORMAT))
    return files

def get_containers():
    client = docker.from_env()
    containers = client.containers.list()
    return containers
