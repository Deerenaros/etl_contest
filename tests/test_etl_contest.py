import pymysql
from .helpers import ping_container

import etl

def test_container_is_alive(mysql_source_image):
    assert ping_container(mysql_source_image)


def test_containers_assets_is_ready(mysql_source_image,
                                    mysql_destination_image):

    src_conn = pymysql.connect(**mysql_source_image,
                               cursorclass=pymysql.cursors.DictCursor)

    with src_conn:
        with src_conn.cursor() as c:
            src_query = """
                SELECT 
                    COUNT(*) AS total 
                FROM transactions t
                    JOIN operation_types ot ON t.idoper = ot.id
            """

            c.execute(src_query)
            src_result = c.fetchone()

    dst_conn = pymysql.connect(**mysql_destination_image,
                               cursorclass=pymysql.cursors.DictCursor)

    with dst_conn:
        with dst_conn.cursor() as c:
            dst_query = """
                SELECT 
                    COUNT(*) AS total 
                FROM transactions_denormalized t
            """

            c.execute(dst_query)
            dst_result = c.fetchone()

    assert src_result['total'] > 0
    assert dst_result['total'] == 0


def test_data_transfer(mysql_source_image,
                       mysql_destination_image):
    """
    :param mysql_source_image: Контейнер mysql-источника с исходными данными
    :param mysql_destination_image: Контейнер mysql-назначения
    :return:
    """


    CHECK_DST = """
        SELECT
            t.id, t.dt, t.idoper, t.move, t.amount, t.name_oper
        FROM
            transactions_denormalized t
        ORDER BY t.dt
    """

    CHECK_SRC = """
        SELECT
            t.id, t.dt, t.idoper, t.move, t.amount, o.name as name_oper
        FROM
            transactions t
        JOIN
            operation_types o
        ON
            t.idoper = o.id
        ORDER BY t.dt
    """

    etl.process(mysql_source_image, mysql_destination_image)

    with pymysql.connect(**mysql_destination_image) as cdst:
                with cdst.cursor() as c:
                    c.execute(CHECK_DST)
                    dst_table = list(c.fetchall())

    with pymysql.connect(**mysql_source_image) as csrc:
                with csrc.cursor() as c:
                    c.execute(CHECK_SRC)
                    src_table = list(c.fetchall())

    import pprint

    assert dst_table == src_table