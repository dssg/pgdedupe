import tempfile

import pandas as pd
import numpy as np


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


def merge(mapping_table, entries_table, exact_columns, con):
    edges = pd.read_sql("""
    with subset as (
        SELECT _unique_id, canon_id, {cols}
        FROM {entries} LEFT JOIN {mapping} using (_unique_id)
    )

    SELECT t1.canon_id id1, id2 from
    subset t1 JOIN
    (SELECT min(canon_id) id2, {cols} from subset group by {cols} having count(*) > 1) t
    using ({cols})
    where t1.canon_id > id2
    group by 1,2
    """.format(cols=', '.join(exact_columns), entries=entries_table, mapping=mapping_table), con)

    components = components_dict_to_df(get_components(edges))

    c = con.cursor()
    with tempfile.TemporaryFile() as f:
        components.to_csv(f, index=False, header=False)
        t = "merged_" + "_".join(exact_columns)
        c.execute("DROP TABLE IF EXISTS dedupe.{}".format(t))  # TODO SCHEMA NAME
        c.execute("CREATE TABLE dedupe.{} (id1 INT, id2 INT)".format(t))  # TODO DATA TYPE
        f.seek(0)
        c.copy_from(f, 'dedupe.' + t, sep=',')
    c.execute("""UPDATE {m} m SET
                     canon_id = t.id1
                 FROM dedupe.{t} t
                 WHERE m.canon_id = t.id2""".format(m=mapping_table, t=t))
    con.commit()
