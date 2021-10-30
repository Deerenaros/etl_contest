class PRIMITIVES:
    BEGIN = """
        SELECT t.dt FROM %(_tname)s t
        ORDER BY t.dt ASC
        LIMIT 1
    """
    END = """
        SELECT t.dt FROM %(_tname)s t
        ORDER BY t.dt DESC
        LIMIT 1
    """

    ACTION = """
        SELECT t.id, t.dt, t.idoper, t.move, t.amount, o.name as name_oper FROM transactions t
        JOIN operation_types o
        ON t.idoper = o.id
        WHERE dt > '%(last_transaction)s' AND dt >= '%(sslice)s' AND dt < '%(sslice+_delta)s'
        ORDER BY t.dt
    """

    CHECK = """
        SELECT t.id, t.dt, t.idoper, t.move, t.amount, t.name_oper FROM transactions_denormalized t
    """

from datetime import date, datetime, timedelta


def datespan(startDate, endDate, delta=timedelta(hours=1)):
    currentDate = startDate
    while currentDate < endDate:
        yield currentDate
        currentDate += delta


class Elevator:
    def __getitem__(self, k):
        import inspect

        back = inspect.currentframe().f_back.f_locals
        return eval(k, globals(), self.__dict__ | back)


class Extractor(Elevator):
    def __init__(self, src, tname, delta=timedelta(hours=1)):
        self._src = src
        self._tname = tname
        self._delta = delta

    def __iter__(self):
        import pymysql

        with pymysql.connect(**self._src) as src:
            with src.cursor() as c:
                c.execute(PRIMITIVES.BEGIN % self)
                begin, = c.fetchone()
                c.execute(PRIMITIVES.END % self)
                end, = c.fetchone()

                last_transaction = datetime(1970, 1, 1, 0, 0, 0)
                begin = max(begin, last_transaction)

                for sslice in datespan(begin, end, self._delta):
                    c.execute(PRIMITIVES.ACTION % self)
                    yield c.fetchall()


class Transformer:
    def __init__(self, *actions):
        self._actions = actions


class Loader(Elevator):
    def __init__(self, src: Extractor, dst, tname):
        self._src = src
        self._dst = dst

    def load(self):
        import pymysql

        with pymysql.connect(**self._dst) as dst:
            with dst.cursor() as c:
                for extracted in self._src:
                    print(extracted)
                    c.executemany(
                        "INSERT INTO transactions_denormalized (id, dt, idoper, move, amount, name_oper) VALUES (%s, %s, %s, %s, %s, %s)",
                        extracted)

def process(src, dst):
    import pprint
    import pymysql

    Loader(Extractor(src, "transactions"), dst, "transactions_denormalized").load()

    with pymysql.connect(**dst) as cdst:
                with cdst.cursor() as c:
                    c.execute(PRIMITIVES.CHECK)
                    pprint.PrettyPrinter().pprint(list(c.fetchall()))
    