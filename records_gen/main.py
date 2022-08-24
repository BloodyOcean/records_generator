from entities import DbGenerator, DbServer


def main():
    dbgen = DbGenerator()
    dbgen.generate(100)
    dbserv = DbServer()
    dbserv.open_connection()
    dbserv.load_records(dbgen.salers + dbgen.orders + dbgen.customers)


if __name__ == '__main__':
    main()
