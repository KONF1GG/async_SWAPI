import datetime
import asyncio
import aiohttp
from more_itertools import chunked
from models import init_orm, Session, SwapiPeople

MAX_REQUESTS = 30

async def fetch_data(session, endpoint):
    response = await session.get(endpoint)
    json_data = await response.json()
    return json_data

async def get_resource_name(session, resource_type, resource_id):
    endpoint = f'https://swapi.py4e.com/api/{resource_type}/{resource_id}/'
    json_data = await fetch_data(session, endpoint)
    return json_data.get('name') or json_data.get('title')

async def fetch_and_join_names(session, resource_type, resource_urls):
    resource_ids = [url.split('/')[-2] for url in resource_urls]
    print(resource_ids)
    name_tasks = [get_resource_name(session, resource_type, resource_id) for resource_id in resource_ids]
    names_list = await asyncio.gather(*name_tasks)
    return ', '.join(names_list)

async def insert_json_list(web_session, json_list):
    async with Session() as db_session:
        for item in json_list:
            if 'detail' not in item:
                try:
                    films = await fetch_and_join_names(web_session, 'films', item.get('films', []))
                    species = await fetch_and_join_names(web_session, 'species', item.get('species', []))
                    starships = await fetch_and_join_names(web_session, 'starships', item.get('starships', []))
                    vehicles = await fetch_and_join_names(web_session, 'vehicles', item.get('vehicles', []))

                    # Process homeworld
                    homeworld_id = item.get('homeworld').split('/')[-2] if item.get('homeworld') else None
                    homeworld_name = await get_resource_name(web_session, 'planets', homeworld_id) if homeworld_id else 'unknown'

                    orm_model = SwapiPeople(
                        id=item.get('id'),
                        birth_year=item.get('birth_year'),
                        eye_color=item.get('eye_color'),
                        films=films,
                        gender=item.get('gender'),
                        hair_color=item.get('hair_color'),
                        height=item.get('height', 'unknown'),
                        homeworld=homeworld_name,
                        mass=item.get('mass', 'unknown'),
                        name=item.get('name'),
                        skin_color=item.get('skin_color'),
                        species=species,
                        starships=starships,
                        vehicles=vehicles
                    )
                    db_session.add(orm_model)
                except Exception as e:
                    print(e)
        await db_session.commit()

async def main():
    await init_orm()
    async with aiohttp.ClientSession() as web_session:
        for people_ids in chunked(range(1, 101), MAX_REQUESTS):
            coros = [fetch_data(web_session, f'https://swapi.py4e.com/api/people/{i}/') for i in people_ids]
            json_list = await asyncio.gather(*coros)
            await insert_json_list(web_session, json_list)

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
