## ETL migration process .

- As for now project can't be run from the container,
     but you can run postges and etl container from dcoker compose as for now . Only ETL transform process HAVE to be run maually  "Working on it "   
- Migration devided into 3 separete processes each can be run separately
- Each process film_work , genres , persons  migrates data to ELT by last updated_at field 
### Current revision fixes 
   <s>pep8 full compliance</s><br>
   <del> code revision and uptimizations</s><br>
   <del>Simple schedule script to  run outomation with interval</del><br>
   <del> Bug fixes</del><br>


## Minimum Requirements
This project supports Ubuntu Linux 18.04  

- [Docker 20.10 +](https://docs.docker.com/)
- [docker-compose  1.29.2 + ](https://docs.docker.com/compose/)
- [elasticsearch 7.16.2 + ]  (https://elasticsearch-py.readthedocs.io/en/v7.16.2/)
- [psycopg2-binary 2.9.3 ]  (https://pypi.org/project/psycopg2-binary/)


 # You can start migration by Runnig this commands . 
 ## ETL transformation process is not included in dcoker compose file yet 
## Quickstart

```bash
$ docker-compose up -d  --bulid 


```
# Yoou  can run postgres in a container separately
## Create postgres volume
```bash
$ sudo docker run -d --rm \
  --name postgres \
  -p 5432:5432 \
  -v /path/to/movies_admin:/var/lib/postgresql/data \
  -e POSTGRES_PASSWORD=<secret_password> \
  postgres:13 


```
#  The same is for Elsticsearch instance
## Run elsticseach in a container
```bash
$ sudo docker run -p 9200:9200 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.7.0 ^C

```
# Notes 
- Config validation through pydantic
- Loger logs all everything  to  a separate es.log  volume .. So  you can mount this logging volume to Filebit
