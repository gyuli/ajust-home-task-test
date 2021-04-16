import csv
from datetime import date
from typing import Optional

from pydantic import BaseModel, Field
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, \
    String, Date, Float, select, ForeignKey, func, case, asc, desc, text
from sqlalchemy.orm import sessionmaker


class IndexResponse(BaseModel):
    title: str = 'Test for Python Backend Engineer @ adjust.com.'
    description: str = 'https://career.habr.com/vacancies/1000073251'
    version: str = '0.0.1'


# from csv data-import model
class RowData(BaseModel):
    id: int = Field(default=None)
    date: date = Optional[date]
    channel: str = Field(...)
    country: str = Field(...)
    operating_system: str = Field(...)
    impressions: int = Field(...)
    clicks: int = Field(...)
    installs: int = Field(...)
    spend: float = Field(...)
    revenue: float = Field(...)


# for analitics query parameters
class GetQueryData(BaseModel):
    fields: str
    group: Optional[str] = Field(default=None)
    order: Optional[str] = Field(default=None)
    date_from: Optional[date] = Field(default=None)
    date_to: Optional[date] = Field(default=None)
    channel: Optional[str] = Field(default=None)
    country: Optional[str] = Field(default=None)
    operating_system: Optional[str] = Field(default=None)
    impressions: Optional[int] = Field(default=None, ge=0)
    clicks: Optional[int] = Field(default=None, ge=0)
    installs: Optional[int] = Field(default=None, ge=0)
    spend: Optional[float] = Field(default=None, ge=0)
    revenue: Optional[float] = Field(default=None, ge=0)
    cpi: Optional[float] = Field(default=None, ge=0)


# for test purpose only, just fill database tables.
def fill_db():
    with open('app/model/sample_data.csv', 'r') as f:
        sample_csv_data_reader = csv.DictReader(f)
        channels, countries, operating_systems = {}, {}, {}
        for row in sample_csv_data_reader:
            row_data = RowData(**row)
            channel_id = channels.get(row_data.channel, None)
            if channel_id is None:
                session.execute(channels_table.insert().values(channel=row_data.channel))
                sel = select([channels_table.c.id]).where(channels_table.c.channel == row_data.channel)
                channel_id = list(session.execute(sel))[0].id
                channels.setdefault(row_data.channel, channel_id)
            country_id = countries.get(row_data.country, None)
            if country_id is None:
                session.execute(countries_table.insert().values(country=row_data.country))
                sel = select([countries_table.c.id]).where(countries_table.c.country == row_data.country)
                country_id = list(session.execute(sel))[0].id
                countries.setdefault(row_data.country, country_id)
            operating_system_id = operating_systems.get(row_data.operating_system, None)
            if operating_system_id is None:
                session.execute(operating_systems_table.insert().values(operating_system=row_data.operating_system))
                sel = select([operating_systems_table.c.id]).\
                    where(operating_systems_table.c.operating_system == row_data.operating_system)
                operating_system_id = list(session.execute(sel))[0].id
                operating_systems.setdefault(row_data.operating_system, operating_system_id)
            session.execute(performance_metrics_table.insert().values(date=row_data.date,
                                                                      channel_id=channel_id,
                                                                      country_id=country_id,
                                                                      operating_system_id=operating_system_id,
                                                                      impressions=row_data.impressions,
                                                                      clicks=row_data.clicks,
                                                                      installs=row_data.installs,
                                                                      spend=row_data.spend,
                                                                      revenue=row_data.revenue))


# the very place where query data is worked over and return queried data
async def get_analitics(query_data) -> list:
    # list of fields and their consiquence template work out,
    # also how to group if needed
    fields_list = query_data.fields.replace(' ', '').split(',')
    group_by_list = [] if query_data.group is None else query_data.group.replace(' ', '').split(',')
    sel_col_list = []
    grp_col_list = []
    for field in fields_list:
        c = tables_fields[field]
        if group_by_list:
            if field in group_by_list:
                sel_col_list.append(c.label(field))
                grp_col_list.append(c)
            else:
                sel_col_list.append(func.sum(c).label(field))

    # working out sorting if needed
    order_by_list = [] if query_data.order is None else query_data.order.split(',')
    ord_col_list = []
    for column in order_by_list:
        sort_func = desc if '-' in column or 'desc' in column else asc
        column = column.replace('+', '').replace('asc', '').replace('-', '').replace('desc', '').replace(' ', '')
        # if column == 'cpi':
        for i in range(len(sel_col_list)):
            if sel_col_list[i].name == column:
                ord_col_list.append(sort_func(text(f'{i + 1}')))
                break
        # else:
        #     c = tables_fields[column]
        #     ord_col_list.append(sort_func(c))

    # forming tables joining for sql-generating
    join_obj = performance_metrics_table.\
        join(channels_table, channels_table.c.id == performance_metrics_table.c.channel_id).\
        join(countries_table, countries_table.c.id == performance_metrics_table.c.country_id).\
        join(operating_systems_table, operating_systems_table.c.id == performance_metrics_table.c.operating_system_id)
    # forming base sql
    sql = select(sel_col_list).select_from(join_obj).group_by(*grp_col_list).order_by(*ord_col_list)
    # applying date period filters
    if query_data.date_from is not None:
        sql = sql.filter(performance_metrics_table.c.date >= query_data.date_from)
    if query_data.date_to is not None:
        sql = sql.filter(performance_metrics_table.c.date <= query_data.date_to)
    # applying other left filters
    filter_dict = {k: v for k, v in query_data.dict().items() if v is not None and k not in special_params_list}
    for column, value in filter_dict.items():
        if group_by_list and column not in group_by_list and column in fields_list:
            sql = sql.having(func.sum(tables_fields[column]) == value)
        else:
            sql = sql.filter(tables_fields[column] == value)
    # executing formes sql and returning fetched data
    return session.execute(sql).all()


# preparing columns info for generating sql
def fill_tables_fields_dict(t_fields) -> None:
    t_fields.update({c.name: c for c in channels_table.c if c.name != 'id'})
    t_fields.update({c.name: c for c in countries_table.c if c.name != 'id'})
    t_fields.update({c.name: c for c in operating_systems_table.c if c.name != 'id'})
    t_fields.update({c.name: c for c in performance_metrics_table.c if c.name != 'id'})
    t_fields['cpi'] = case([(performance_metrics_table.c.installs == 0, performance_metrics_table.c.spend)],
                           else_=performance_metrics_table.c.spend / performance_metrics_table.c.installs)


# starting DB
engine = create_engine('sqlite:///:memory:')
metadata = MetaData(engine)
# making tables
channels_table = Table('channels', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('channel', String, unique=True, nullable=False))
countries_table = Table('countries', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('country', String, unique=True, nullable=False))
operating_systems_table = Table('operating_systems', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('operating_system', String, unique=True, nullable=False))
performance_metrics_table = Table('performance_metrics', metadata,
                                  Column('id', Integer, primary_key=True),
                                  Column('date', Date, index=True, nullable=False),
                                  Column('channel_id', Integer, ForeignKey('channels.id'), nullable=False),
                                  Column('country_id', Integer, ForeignKey('countries.id'), nullable=False),
                                  Column('operating_system_id', Integer, ForeignKey('operating_systems.id'),
                                         nullable=False),
                                  Column('impressions', Integer, nullable=False),
                                  Column('clicks', Integer, nullable=False),
                                  Column('installs', Integer, nullable=False),
                                  Column('spend', Float, nullable=False),
                                  Column('revenue', Float, nullable=False))
metadata.create_all()
# filling special params not to use in filter in loop
special_params_list = ['fields', 'group', 'order', 'date_from', 'date_to']
tables_fields = {}
fill_tables_fields_dict(t_fields=tables_fields)
# opening session
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()
# filling DB with data
fill_db()
