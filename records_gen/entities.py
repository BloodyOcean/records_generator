import datetime
import itertools
import logging
import random

import mimesis
from mimesis.enums import Locale
from sqlalchemy import Column, Integer, DateTime, Float
from sqlalchemy import String
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base
from sqlalchemy_utils import database_exists, create_database

import config_helper

Base = declarative_base()


class Order(Base):
    __tablename__ = "orders"
    onum = Column(Integer, primary_key=True)
    amt = Column(Float, default=0)
    odate = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    cnum = Column(Integer, nullable=False)
    snum = Column(Integer, nullable=False)
    id_counter = itertools.count()

    def __init__(self, n1: int, n2: int):
        super(Order, self).__init__()
        self.onum = next(self.id_counter)
        self.amt = random.Random().randint(0, 5000) / 1000
        self.odate = mimesis.Datetime().datetime()
        self.cnum = n1
        self.snum = n2

    def __str__(self):
        return f'{self.onum} {self.amt} {self.odate} {self.cnum} {self.snum}'


class Saler(Base):
    __tablename__ = "salers"
    id_counter1 = itertools.count()
    snum = Column(Integer, primary_key=True)
    sname = Column(String(30), nullable=False)
    city = Column(String(30), nullable=False)
    comm = Column(Float, nullable=False)

    def __str__(self):
        return f'{self.snum} {self.sname} {self.city} {self.comm}'

    def __init__(self):
        super(Saler, self).__init__()
        self.snum = next(self.id_counter1)
        self.sname = mimesis.Person(Locale.EN).name()
        self.city = mimesis.Address(Locale.EN).city()
        self.comm = random.Random().randint(0, 5000) / 1000


class Customer(Base):
    __tablename__ = "customers"
    id_counter2 = itertools.count()
    cname = Column(String)
    cnum = Column(Integer, primary_key=True)
    city = Column(String)
    rating = Column(Float)

    def __str__(self):
        return f'{self.cnum} {self.cname} {self.city} {self.rating}'

    def __init__(self):
        super(Customer, self).__init__()
        self.cnum = next(self.id_counter2)
        self.cname = mimesis.Person(Locale.EN).name()
        self.city = mimesis.Address(Locale.EN).city()
        self.rating = random.Random().randint(0, 5000) / 1000


class DbGenerator:
    def __init__(self):
        self.orders = []
        self.salers = []
        self.customers = []

    def generate(self, n: int) -> None:
        self.salers = [Saler() for _ in range(n)]
        self.customers = [Customer() for _ in range(n)]
        self.orders = [Order(random.Random().choice(self.salers).snum,
                             random.Random().choice(self.customers).cnum) for _ in range(n)]


class DbServer:
    def __init__(self):
        self.connection_string = None
        self.engine = None
        self.helper = config_helper.ConfigReader()

    def open_connection(self):
        """
        Tries to open connection to existed db by connection string from config file
        :return:
        """
        logging.info('Trying to get connection string from config file DbServer open_connection()')
        self.connection_string = self.helper.read_connection_string()

        try:
            logging.info('Trying to create engine DbServer open_connection()')
            self.engine = create_engine(self.connection_string, echo=True, future=True)

            try:
                if not database_exists(self.connection_string):
                    logging.info(
                        f'Creating a new database with corresponding parameters: '
                        f'{self.engine.url} DbServer open_connection()')
                    logging.info('Database created successfully DbServer open_connection()')
            except Exception:
                create_database(self.engine.url)

            logging.info('Trying to create migration DbServer open_connection()')
            Base.metadata.create_all(self.engine)
            logging.info('Migration created successfully DbServer open_connection()')
        except Exception:
            logging.error('Cant connect or migration fails DbServer open_connection()')
            raise ValueError('Invalid db connection')

    def load_records(self, lst: list):
        """
        Tries to load all the records to existing database
        :param lst: extended list of each model
        :return:
        """
        logging.info('Starting new session DbServer load_records()')
        with Session(self.engine) as session:
            logging.info('Adding models to db DbServer load_records()')
            session.add_all(lst)
            logging.info('Committing DbServer load_records()')
            session.commit()
            logging.info(f'Committed successfully, size: {len(lst)} DbServer load_records()')

            # refreshing objects of the current session
            logging.info('Trying to refresh all the items DbServer load_records()')
            for item in lst:
                session.refresh(item)
        logging.info('Working with session finished successfully DbServer load_records()')
