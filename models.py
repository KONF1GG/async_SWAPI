import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from sqlalchemy import Integer, JSON, String, Text
from dotenv import load_dotenv

load_dotenv()

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5431")


PG_DSN = (f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
          f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

engine = create_async_engine(PG_DSN)
Session = async_sessionmaker(bind=engine, expire_on_commit=False)

class Base(DeclarativeBase, AsyncAttrs):
    pass

class SwapiPeople(Base):
    __tablename__ = 'swapi'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    birth_year: Mapped[str] = mapped_column(String)
    eye_color: Mapped[str] = mapped_column(String)
    films: Mapped[str] = mapped_column(Text)
    gender: Mapped[str] = mapped_column(String)
    hair_color: Mapped[str] = mapped_column(String)
    height: Mapped[str] = mapped_column(String)
    homeworld: Mapped[str] = mapped_column(String)
    mass: Mapped[str] = mapped_column(String)
    name: Mapped[str] = mapped_column(String)
    skin_color: Mapped[str] = mapped_column(String)
    species: Mapped[str] = mapped_column(Text)
    starships: Mapped[str] = mapped_column(Text)
    vehicles: Mapped[str] = mapped_column(Text)

async def init_orm():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)