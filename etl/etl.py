import pymysql

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

    def sync(self, dst: "Loader"):
        self._last_transaction = dst.sync()

    def __iter__(self):
        with pymysql.connect(**self._src) as src:
            with src.cursor() as c:
                c.execute(PRIMITIVES.BEGIN % self)
                begin, = c.fetchone()
                c.execute(PRIMITIVES.END % self)
                end, = c.fetchone()

                begin = max(begin, self._last_transaction)

                for sslice in datespan(begin, end+self._delta, self._delta):
                    c.execute(PRIMITIVES.EXTRACT % self)
                    for line in c.fetchall():
                        yield line


class Loader(Elevator):
    def __init__(self, src: Extractor, dst: dict[str, Any], tname: str):
        self._tname = tname
        self._src = src
        self._dst = dst

    def sync(self):
        with pymysql.connect(**self._dst) as dst:
            with dst.cursor() as c:
                c.execute(PRIMITIVES.BEGIN % self)
                try:
                    begin, = c.fetchone()
                except TypeError:
                    begin = datetime(1, 1, 1)
        return begin

    def load(self):
        self._src.sync(self)

        with pymysql.connect(**self._dst) as dst:
            with dst.cursor() as c:
                for extracted in self._src:
                    c.execute(PRIMITIVES.LOAD, extracted)