from fastapi import FastAPI, Depends, Response, status

from app.model import IndexResponse, GetQueryData, get_analitics

app = FastAPI(**IndexResponse().dict())


# index path
@app.get('/', tags=['index'])
async def index() -> dict:
    return {'details': IndexResponse()}


# path to get needed analitics
@app.get('/api/data', tags=['analitics'])
async def get_data(response: Response, query_data: GetQueryData = Depends(), v: int = 1) -> dict:
    if v == 1:
        data = await get_analitics(query_data=query_data)
        return {'result': {'items': len(data), 'data': data}}
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {'detail': f'Unsupported version {v}'}

# examples queries from test terms
# 1. http://localhost:8080/api/data?fields=channel,country,impressions,clicks&group=channel,country&order=clicks-&date_to=2017-06-01
# 2. http://localhost:8080/api/data?fields=date,installs&group=date&order=date&date_from=2017-05-01&date_to=2017-05-31&operating_system=ios
# 3. http://localhost:8080/api/data?fields=operating_system,revenue&group=operating_system&order=revenue-&date_from=2017-06-01&date_to=2017-06-01
# 4. http://localhost:8080/api/data?fields=channel,cpi,spend&group=channel&order=cpi-
