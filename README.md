# dabitup (DA-tabase-IT-UP) 🛠
Quickly deploy dockerized databases and insert tables stored in text files. Great for developing data science pipelines and mocking databases for testing purposes.
#
### Quickstart

Spin up a psql docker container and insert the data in ./data/*.csv

```
$ cd /path/to/this/repo
$ python -m pip install -r requirements.txt
$ python -m dabitup sql ./data user dontusethispassword --fformat .csv
```
Spin up a mongo docker container and insert the data in ./loadsofjson/*.json.

Please note that there is no loadsofjson folder provided.
There is a csv file in /data to loads into psql with the command listed above.

```
$ python -m dabitup mongo ./loadsofjson/ user dontusethispassword --fformat .json
```

### Dependencies
Must have docker and docker-compose installed and able to execute ```docker ps``` without sudo.

