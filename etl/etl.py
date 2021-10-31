from typing import Iterable, Any

from .constants import PRIMITIVES
from .utils import datespan, timedelta, datetime


class Elevator:
    def __getitem__(self, k):
        import inspect

        back = inspect.currentframe().f_back.f_locals
        return eval(k, globals(), self.__dict__ | back)


class Extractor(Elevator):
    def __init__(self, src: Iterable, tname: str, delta: timedelta = timedelta(hours=1)):
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

                for sslice in datespan(begin, end+self._delta, self._delta):
                    c.execute(PRIMITIVES.EXTRACT % self)
                    for line in c.fetchall():
                        yield line


class Loader(Elevator):
    def __init__(self, src: Iterable, dst: dict[str, Any], tname: str):
        self._src = src
        self._dst = dst

    def load(self):
        import pymysql

        with pymysql.connect(**self._dst) as dst:
            with dst.cursor() as c:
                for extracted in self._src:
                    c.execute(PRIMITIVES.LOAD, extracted)