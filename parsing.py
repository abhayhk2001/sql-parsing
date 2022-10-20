from sqlglot import parse_one, exp
from helpers import files, facts, dimensions, features, get_files
import pandas as pd
import numpy as np
import re

files = files
files = get_files()

df = pd.DataFrame(data=np.zeros(
    (len(files), len(features)), int), columns=features)
i = 0
for file in files:
    with open('./queries/'+file, 'r') as fd:
        sqlFile = fd.read()
    sqlCommands = sqlFile.split(';')
    for command in sqlCommands:
        parsed = parse_one(command)
        if (parsed == None):
            continue
        tables = parsed.find_all(exp.Table)
        for table in tables:
            name = table.alias_or_name
            if (name in facts):
                df.at[i, 'table-fact'] += 1
            elif (name in dimensions):
                df.at[i, 'table-dimension'] += 1
            else:
                print(name)

        joins = parsed.find_all(exp.Join)
        for join in joins:
            parent = join.parent_select.sql().split(' ')
            table1 = parent[parent.index("FROM")+1]
            table2 = join.alias_or_name
            col = ('ff join' if ((name in facts) and (
                name in dimensions)) else 'df join')

            df.at[i, 'joins'] = join.side+'_'+join.kind
            df.at[i, col] += 1

        subqueries = parsed.find_all(exp.Subquery)
        for query in subqueries:
            df.at[i, 'subquery'] += 1

        aggregates = parsed.find_all(exp.AggFunc)
        for aggregate in aggregates:
            col = aggregate.sql_name().lower()
            df.at[i, col] += 1

        wheres = parsed.find_all(exp.Where)
        for where in wheres:
            df.at[i, 'where'] += 1
            conditions = len(re.split('AND|OR', where.sql()[6:]))
            df.at[i, 'largest where'] = conditions if df.at[i,
                                                            'largest where'] < conditions else df.at[i, 'largest where']
        groups = parsed.find_all(exp.Group)
        for group in groups:
            df.at[i, 'group'] += 1
            columns = len(list(group.find_all(exp.Column)))
            df.at[i, 'largest group'] = columns if df.at[i,
                                                         'largest group'] < columns else df.at[i, 'largest group']

        orders = parsed.find_all(exp.Order)
        for order in orders:
            df.at[i, 'order'] += 1
            columns = len(list(order.find_all(exp.Column)))
            df.at[i, 'largest order'] = columns if df.at[i,
                                                         'largest order'] < columns else df.at[i, 'largest order']

        limits = parsed.find_all(exp.Limit)
        for limit in limits:
            df.at[i, 'limit'] += 1
            rows = int(limit.expression.__str__())
            df.at[i, 'largest limit'] = rows if df.at[i,
                                                      'largest limit'] < rows else df.at[i, 'largest limit']
        i += 1
        df.loc[i, :] = int(0)
    # sorts = parsed.find_all(exp.Sort)
    # for sort in sorts:
    #     print(sort)
    #     df.at[i, 'sort'] += 1
    #     columns = len(list(sort.find_all(exp.Column)))
    #     df.at[i, 'largest sort'] = columns if df.at[i,
    #                                                 'largest sort'] < columns else df.at[i, 'largest sort']


df = df[:-1]
print(df)
df.to_csv('results.csv')
