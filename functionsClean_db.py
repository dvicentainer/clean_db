import psycopg2
import sqlalchemy as sa
from pandas.api.types import is_string_dtype, is_numeric_dtype, is_datetime64_any_dtype
import geopandas as gpd
import pyinputplus as pyp
import pandas as pd
import sys


def loging_db():
    host = input('host: ')
    if not host:
        user_input = 'localhost'
    user_name = input('user_name: ')
    if not user_name:
        user_input = 'postgres'
    pgpassword = input('password: ')
    db_name = input('db_name: ')

    conn_string = f"host={host} port=5432 dbname={db_name} user={user_name} password={pgpassword}"
    conn = psycopg2.connect(conn_string)
    engine = sa.create_engine(f'postgresql://{user_name}:{pgpassword}@{host}:5432/{db_name}')

    return conn, engine, db_name

def db_size(db_name, curr):
    selectDB_size = """SELECT pg_size_pretty(pg_database_size('{}'))""".format(db_name)
    curr.execute(selectDB_size)
    database_size = curr.fetchall()[0]
    print(database_size)
    return database_size

def table_size(curr, schema_name, table_name):
    select_table_size = """SELECT pg_size_pretty(pg_total_relation_size('{}.{}'))""".format(schema_name, table_name)
    curr.execute(select_table_size)
    db_table_size = curr.fetchall()[0]
    print(db_table_size)
    return db_table_size

def see_schemas(curr):
    curr.execute('''SELECT schema_name FROM information_schema.schemata;''')
    print('Listing schemas on database...')
    schemas_list = []
    for schema_name in curr.fetchall():
        print(schema_name)
        schemas_list.append(schema_name[0])

    return schemas_list

def see_tables(curr, schemas_list):
    schema_name = input("Work on Schema: ")

    if schema_name not in schemas_list:
        sys.exit()
    curr.execute("""SELECT table_name FROM information_schema.tables
           WHERE table_schema = '{}'""".format(schema_name))

    print('Loading tables in {} schema'.format(schema_name))

    table_list = []
    for table in curr.fetchall():
        print(table)
        table_list.append(table[0].strip(','))

    return schema_name, table_list

def load_pd(sql_command, conn):
    gdf = pd.read_sql_query(sql_command, conn)

    print(gdf)
    return gdf

def load_gpd(sql_command, conn):
    gdf = gpd.GeoDataFrame.from_postgis(sql_command, conn, geom_col='geom')

    print(gdf)
    return gdf

def check_geomDimension(schema_name, table_name, curr):
    sql_command = "SELECT ST_NDims(geom) FROM {}.{} limit 1;".format(schema_name, table_name)
    curr.execute(sql_command)
    geom_dimension = curr.fetchall()[0]
    return geom_dimension

def allclean_columns(gdf):
    col_list = []

    for col in gdf.columns:
        col = col.replace('.', '_')
        col_list.append(col)
    gdf.columns = col_list

    return gdf, gdf.columns

def load_data(schema_name, conn, table_list, curr):
    table_name = input("Table Name: ")
    print(table_list)
    if table_name not in table_list:
        sys.exit()

    sql_command = "SELECT * FROM {}.{};".format(schema_name, table_name)
    print(sql_command + '\n')

    print('Does the table have geometry? ')
    actions_list = ['Yes', 'No']
    action = pyp.inputMenu(actions_list, numbered=True)
    print(action)

    if action == actions_list[0]:
        gdf = load_gpd(sql_command, conn)
        geom_dimension = check_geomDimension(schema_name, table_name, curr)

        return gdf, table_name, geom_dimension[0]

    elif action == actions_list[1]:
        gdf = load_pd(sql_command, conn)

        return gdf, table_name, 0

def saveGeoCol(gdf):
    for col, dt in gdf.dtypes.items():
        if dt == 'geometry':
            return col
        else:
            print("This table doesn't have a geometry!")

def drop_exceptColumn(gdf):
    remain_list = [c.strip(' ') for c in input("Wich columns will be kept? ").split(",")]
    excluded_columns = gdf.columns.difference(remain_list)

    gdf.drop(gdf.columns.difference(remain_list), 1, inplace=True)

    return excluded_columns, remain_list

def drop_Column(gdf):
    excluded_columns = [c.strip(' ') for c in input("Wich columns to delete? ").split(",")]
    gdf.drop(excluded_columns, 1, inplace=True)
    remain_list = gdf.columns.difference(excluded_columns)

    return excluded_columns, remain_list

def db_action(gdf, all_columns):
    print('How to proceed?')
    actions_list = ['Choose remaining columns', 'Choose columns to delete', 'Keep current table']
    action = pyp.inputMenu(actions_list, numbered=True)
    print(action)

    if action == actions_list[0]:
        print(all_columns)
        return drop_exceptColumn(gdf)

    elif action == actions_list[1]:
        print(all_columns)
        return drop_Column(gdf)

    elif action == actions_list[2]:
        excluded_columns = 'Nothing has changed'
        remain_list = 'Nothing has changed'
        return excluded_columns, remain_list

def drop_oldDB(schema_name, table_name, curr):
    dropTableStmt = "DROP TABLE {}.{}".format(schema_name, table_name)
    print(f'Dropping {table_name}...\n')
    curr.execute(dropTableStmt)

def drop_nageom(k, gdf):
    if len(gdf[gdf[k] == None]) != 0:
        list_nageom = gdf[gdf[k] == None].index.tolist()
        gdf.drop(labels=list_nageom, axis=0, inplace=True)
    else:
        pass

def geomType(k, gdf, geom_dimension):
    drop_nageom(k, gdf)

    if geom_dimension == 3:
        sql_add = "{} geometry(MultiLineStringZ, {}) not null".format(k, gdf.crs.to_epsg())
    else:
        sql_add = "{} geometry({}, {}) not null".format(k, gdf.geometry.geom_type.unique()[0], gdf.crs.to_epsg())

    return sql_add

# precisa melhorar, mtos vão como text, mas tem algum erro no meio
def len_varchar(k, gdf):
    len_list = [100]
    for i in gdf[k]:
        try:
            len_list.append(len(i))
        except TypeError:
            pass

    return max(len_list)

def text_strType(k, gdf):
    txt_size = len_varchar(k, gdf)
    sql_add = f"{k} VARCHAR({txt_size})"
    return sql_add


def numericType(k, gdf):
    num_size_max = max(gdf[k])
    num_size_min = min(gdf[k])

    #  4 bytes datatypes
    if type(num_size_max) is int:
        if num_size_max in range(-32768, 32768) and num_size_min in range(-32768, 32768):
            sql_add = f"{k} SMALLINT"
            return sql_add

        elif num_size_max > 32767 or num_size_min < -32768:
            sql_add = f"{k} INTEGER"
            return sql_add

        elif num_size_max > 2147483647 or num_size_min < -2147483648:
            sql_add = f"{k} BIGINT"
            return sql_add

    elif type(num_size_max) is float:

        len_listBeforeDot = [len(str(i).split('.')[0]) for i in gdf[k]]

        try:
            len_listAfterDot = [len(str(i).split('.')[1]) for i in gdf[k]]

        except IndexError:
            len_listAfterDot = [0]

        if max(len_listBeforeDot) < 6 and max(len_listAfterDot) < 6:
            sql_add = f"{k} REAL"
            return sql_add

        else:
            sql_add = f"{k} DOUBLE PRECISION"
            return sql_add


def datetimedType(k, gdf):
    sql_add = f"{k} TIMESTAMPTZ"
    return sql_add


def construct_sql(gdf, curr, conn, schema_name, table_name, geom_dimension):
    #  create list for all sql code
    sql_newCode = []

    #  loop over the remaining columns and values
    for k, v in gdf.dtypes.items():

        if str(type(gdf[k].dtypes)) == "<class 'geopandas.array.GeometryDtype'>":
            sql_add = geomType(k, gdf, geom_dimension)
            sql_newCode.append(sql_add)

        #  create text type
        elif is_string_dtype(gdf[k]):
            sql_add = text_strType(k, gdf)
            sql_newCode.append(sql_add)

        #  create numeric types based on datatype
        elif is_numeric_dtype(gdf[k]):
            sql_add = numericType(k, gdf)
            sql_newCode.append(sql_add)

        elif is_datetime64_any_dtype(gdf[k]):
            sql_add = datetimedType(k, gdf)
            sql_newCode.append(sql_add)

        # TODO -- ADD DATATYPES

    insert_dtypes = ', '.join(sql_newCode)

    return insert_dtypes

def create_table_cmd(schema_name, table_name, insert_dtypes, curr, conn):
    create_table_command = """CREATE TABLE {}.{} ({})""".format(schema_name, table_name, insert_dtypes)
    print(create_table_command)
    curr.execute(create_table_command)
    print("Table created successfully........")
    conn.commit()


def load_gdf2pg(gdf, engine, table_name, schema_name):
    dt = [dt for col, dt in gdf.dtypes.items()]

    if 'geometry' in dt:
        gdf.to_postgis(table_name, engine, schema=schema_name, if_exists='append', index=False)

    else:
        gdf.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False)

def writeMeta(db_name, database_size, table_name, table_size_0, table_size_1, excluded_columns, remain_list):
    report = f"The DataBase {db_name} was {database_size} in size.\n" \
             f"The table {table_name} was {table_size_0} in size. Now it is {table_size_1} in size.\n" \
             f"The following columns were excluded: {excluded_columns}. \n" \
             f"And the remaining columns are {remain_list}."
    with open(f'{db_name}_{table_name}.txt', 'w') as f:
        f.writelines(report)
