import datetime
import asyncio
import aiohttp
from more_itertools import chunked
from models import init_orm, Session, SwapiPeople

MAX_REQUESTS = 30

async def get_people(session, person_id):
    response = await session.get(f'https://swapi.py4e.com/api/people/{person_id}/')
    json_data = await response.json()
    return json_data

async def get_film_title(session, film_id):
    response = await session.get(f'https://swapi.py4e.com/api/films/{film_id}/')
    json_data = await response.json()
    return json_data['title']

async def get_species_name(session, species_id):
    response = await session.get(f'https://swapi.py4e.com/api/species/{species_id}/')
    json_data = await response.json()
    return json_data['name']

async def get_starship_name(session, starship_id):
    response = await session.get(f'https://swapi.py4e.com/api/starships/{starship_id}/')
    json_data = await response.json()
    return json_data['name']

async def get_vehicle_name(session, vehicle_id):
    response = await session.get(f'https://swapi.py4e.com/api/vehicles/{vehicle_id}/')
    json_data = await response.json()
    return json_data['name']

async def get_homeworld_name(session, homeworld_id):
    response = await session.get(f'https://swapi.py4e.com/api/planets/{homeworld_id}/')
    json_data = await response.json()
    return json_data['name']

async def insert_json_list(web_session, json_list):
    async with Session() as db_session:
        for item in json_list:
            if 'detail' not in item:
                try:
                    # Process films
                    film_ids = [film.split('/')[-2] for film in item.get('films', [])]
                    film_title_tasks = [get_film_title(web_session, film_id) for film_id in film_ids]
                    film_titles_list = await asyncio.gather(*film_title_tasks)
                    films = ', '.join(film_titles_list)

                    # Process species
                    species_ids = [species.split('/')[-2] for species in item.get('species', [])]
                    species_name_tasks = [get_species_name(web_session, species_id) for species_id in species_ids]
                    species_names_list = await asyncio.gather(*species_name_tasks)
                    species = ', '.join(species_names_list)

                    # Process starships
                    starship_ids = [starship.split('/')[-2] for starship in item.get('starships', [])]
                    starship_name_tasks = [get_starship_name(web_session, starship_id) for starship_id in starship_ids]
                    starship_names_list = await asyncio.gather(*starship_name_tasks)
                    starships = ', '.join(starship_names_list)

                    # Process vehicles
                    vehicle_ids = [vehicle.split('/')[-2] for vehicle in item.get('vehicles', [])]
                    vehicle_name_tasks = [get_vehicle_name(web_session, vehicle_id) for vehicle_id in vehicle_ids]
                    vehicle_names_list = await asyncio.gather(*vehicle_name_tasks)
                    vehicles = ', '.join(vehicle_names_list)

                    # Process homeworld
                    homeworld_id = item.get('homeworld').split('/')[-2] if item.get('homeworld') else None
                    homeworld_name = await get_homeworld_name(web_session, homeworld_id) if homeworld_id else 'unknown'

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
            coros = [get_people(web_session, i) for i in people_ids]
            json_list = await asyncio.gather(*coros)
            await insert_json_list(web_session, json_list)

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
