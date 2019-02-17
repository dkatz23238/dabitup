import argparse
from dabitup import make_docker_config, insert_tables_to_SQL, insert_records_to_mongo
import time
import subprocess
import os
import glob
import yaml

parser = argparse.ArgumentParser()
parser.add_argument("Database", type=str,
                    help="Database to spin up: mongo or SQL")
parser.add_argument(
    "SourceFiles", help="Source files to INSERT to database once instantiated.")
parser.add_argument("Username", help="Username for database")
parser.add_argument(
    "Password", help="Password to use for user authenticated to database")
parser.add_argument(
    "--fformat", help="Format of files that stored in source directory", dest="fformat", default="DEFAULT")
args = parser.parse_args()

DATABASE = args.Database.lower()
DATA_SOURCE = args.SourceFiles
USERNAME = args.Username
PASSWORD = args.Password
SERVICE_NAME = "db"
FFORMAT = args.fformat

if args.fformat == "DEFAULT":
    if DATABASE == "mongo":
        FFORMAT = ".json"
    elif DATABASE == "SQL":
        FFORMAT = ".csv"


if not os.path.exists(DATA_SOURCE):
    raise Exception("DATA_SOURCE does not exist")

if len(glob.glob(os.path.join(DATA_SOURCE, "*%s" % FFORMAT))) == 0:
    raise Exception("No files found in DATA_SOURCE")

config = make_docker_config(DATABASE, DATA_SOURCE,
                            USERNAME, PASSWORD, SERVICE_NAME)

with open("docker-compose.yml", "w") as f:
    yaml.dump(config, f, default_flow_style=False)

print("writing docker-compose.yml to ./ ")
time.sleep(2)
subprocess.Popen(["docker-compose", "up", "-d"])

time.sleep(10)
print("Sleeping for 10 seconds")

if DATABASE == "mongo":
    time.sleep(10)
    insert_records_to_mongo(
        DATA_SOURCE=DATA_SOURCE, USERNAME=USERNAME, PASSWORD=PASSWORD, FFORMAT=FFORMAT)
elif DATABASE == "sql":
    insert_tables_to_SQL(DATA_SOURCE=DATA_SOURCE,
                         USERNAME=USERNAME, PASSWORD=PASSWORD, FFORMAT=FFORMAT)

print("Complete!")
