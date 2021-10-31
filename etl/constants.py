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

    EXTRACT = """
        SELECT
            t.id, t.dt, t.idoper, t.move, t.amount, o.name as name_oper
        FROM
            transactions t
        JOIN
            operation_types o
        ON
            t.idoper = o.id
        WHERE
            dt > '%(last_transaction)s' AND dt >= '%(sslice)s' AND dt < '%(sslice+_delta)s'
        ORDER BY t.dt
    """

    LOAD = """
        INSERT INTO transactions_denormalized
            (id, dt, idoper, move, amount, name_oper)
        VALUES
            (%s, %s, %s, %s, %s, %s)
    """
