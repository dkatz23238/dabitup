# dabitup
Quickly deploy dockerized database and insert tables stored in text files. Great for developing data science pipelines.

# Quickstart

Spin up a psql docker container and insert the data in ./data/*.csv

```
$ python -m dabitup sql ./data user dontusethispassword --fformat .csv
```
