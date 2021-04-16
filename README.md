# ajust-home-task-test
Test for AjustHomeTask

reqired packages:

pip install uvicorn

pip install fastapi

pip install sqlalchemy

Python 3.7 and higher required.
To start service: $python service.py

To make test queries go (in a browser) to 'http://localhost:8080/docs' or to 'http://localhost:8080/redoc'. It improved with 'openapi.json' 'swagger' and 'ReDoc'. There are opportunities to test any query.

Here are some rquired in test queries:
1. http://localhost:8080/api/data?fields=channel,country,impressions,clicks&group=channel,country&order=clicks-&date_to=2017-06-01
2. http://localhost:8080/api/data?fields=date,installs&group=date&order=date&date_from=2017-05-01&date_to=2017-05-31&operating_system=ios
3. http://localhost:8080/api/data?fields=operating_system,revenue&group=operating_system&order=revenue-&date_from=2017-06-01&date_to=2017-06-01
4. http://localhost:8080/api/data?fields=channel,cpi,spend&group=channel&order=cpi-
