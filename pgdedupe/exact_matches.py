# -*- coding: utf-8 -*-

import csv
import tempfile

import numpy as np

import pandas as pd


def follow(id1, edges, visited=None, weak=True):
    if visited is None:
        visited = set()
    visited.add(id1)

    for row in edges[edges['id1'] == id1].values:
        if(row[1] not in visited):
            follow(row[1], edges, visited)

    if weak:
        for row in edges[edges['id2'] == id1].values:
            if(row[0] not in visited):
                follow(row[0], edges, visited)

    return visited


# if sparse (a lot of disconnected vertices) find those separately (faster)
def get_components(edges, vertices=None):
    if vertices is None:
        vertices = pd.DataFrame({'id': pd.concat((edges['id1'], edges['id2'])).unique()})

    visited = set()
    components = {}

    for id1 in vertices.values[:, 0]:
        if id1 not in visited:
            c = follow(id1, edges)
            visited.update(c)
            components[id1] = c

    return components


def components_dict_to_df(components):
    deduped = np.empty((0, 2), dtype=int)

    for id1 in components:
        deduped = np.append(deduped, [[id1, id2] for id2 in components[id1]], axis=0)

    deduped = pd.DataFrame(deduped, columns=['id1', 'id2'])
    return deduped


def merge(mapping_table, mapping_id,
          entries_table, entry_id,
          exact_columns, schema, con):
    """
    Given a mapping table that identifies clusters of entries in an entry table
    that are linked together, use a subset of columns to perform exact record-
    linkage in order to join together matched clusters.

    Arguments:
        mapping_table: a SQL table that contains two columns, linking each entry_id to a mapping_id
        mapping_id: the column name for the clusters of joined entries
        entries_table: a SQL table that contains the entry_id and columns
        entry_id: the column name for the primary key of the entries_table
        exact_columns: a list of column names over which the exact merge should be performed
        schema: the schema where a temporary table may be created
        con: a connection to the database
    """
    edges = pd.read_sql("""
    with subset as (
        SELECT {key}, {cluster}, {cols}
        FROM {entries} LEFT JOIN {mapping} using ({key})
    )

    SELECT t1.{cluster} id1, id2 from
    subset t1 JOIN
    (SELECT min({cluster}) id2, {cols} from subset group by {cols} having count(*) > 1) t
    using ({cols})
    where t1.{cluster} > id2
    group by 1,2
    """.format(cols=', '.join(exact_columns), entries=entries_table,
               mapping=mapping_table, key=entry_id, cluster=mapping_id), con)

    components = components_dict_to_df(get_components(edges))

    c = con.cursor()
    with tempfile.TemporaryFile(mode='w+t') as f:
        components.to_csv(f, index=False, header=False)
        t = schema + ".merged_" + "_".join(exact_columns)
        c.execute("DROP TABLE IF EXISTS {}".format(t))
        c.execute("""CREATE TABLE {t} AS
                     (SELECT {id} as id1, {id} as id2 FROM {m} LIMIT 0)
                  """.format(t=t, id=mapping_id, m=mapping_table))
        f.seek(0)
        c.copy_from(f, t, sep=',')
    c.execute("""UPDATE {m} m SET
                     {id} = t.id1
                 FROM {t} t
                 WHERE m.{id} = t.id2""".format(m=mapping_table, id=mapping_id, t=t))
    con.commit()


def merge_mssql(mapping_table, mapping_id,
                entries_table, entry_id,
                exact_columns, schema, con):
    """
    Given a mapping table that identifies clusters of entries in an entry table
    that are linked together, use a subset of columns to perform exact record-
    linkage in order to join together matched clusters.

    Arguments:
        mapping_table: a SQL table that contains two columns, linking each entry_id to a mapping_id
        mapping_id: the column name for the clusters of joined entries
        entries_table: a SQL table that contains the entry_id and columns
        entry_id: the column name for the primary key of the entries_table
        exact_columns: a list of column names over which the exact merge should be performed
        schema: the schema where a temporary table may be created
        con: a connection to the database
    """
    exact_columns_join = ' AND '.join(['t.{} = t1.{}'.format(x, x) for x in exact_columns])
    edges = pd.read_sql("""
    with subset as (
        SELECT {entries}.{key}, {cluster}, {cols}
        FROM {entries} LEFT JOIN {mapping} ON {entries}.{key} = {mapping}.{key}
    )

    SELECT t1.{cluster} AS id1, id2 from
    subset AS t1 JOIN
    (SELECT min({cluster}) as id2, {cols} from subset group by {cols} having count(*) > 1) AS t
    ON {exact_columns_join}
    where t1.{cluster} > id2
    group by t1.{cluster}, id2
    """.format(cols=', '.join(exact_columns), exact_columns_join=exact_columns_join,
               entries=entries_table, mapping=mapping_table,
               key=entry_id, cluster=mapping_id), con)

    components = components_dict_to_df(get_components(edges))

    c = con.cursor()
    with tempfile.TemporaryFile(mode='w+t') as f:
        components.to_csv(f, index=False, header=False)
        t = schema + ".merged_" + "_".join(exact_columns)
        c.execute("IF OBJECT_ID('{}', 'U') IS NOT NULL DROP TABLE {}".format(t, t))
        c.execute("""SELECT TOP 0 {id} as id1, {id} as id2 INTO {t} FROM {m}""".format(t=t,
                  id=mapping_id, m=mapping_table))
        f.seek(0)
        reader = csv.reader(f)
        data = next(reader)
        query = "INSERT INTO {}".format(t)
        query = query + " VALUES ({0})".format(','.join(['%s'] * len(data)))
        cc = con.cursor()
        cc.execute(query, tuple(data))
        for data in reader:
            cc.execute(query, tuple(data))

    c.execute("""UPDATE {m}
                 SET {id} = t.id1
                 FROM {t} AS t
                 WHERE {id} = t.id2""".format(m=mapping_table, id=mapping_id, t=t))
    con.commit()
