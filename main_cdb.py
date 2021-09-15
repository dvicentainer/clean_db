from functionsClean_db import *

conn, engine, db_name = loging_db()

with conn.cursor() as curr:
    database_size = db_size(db_name, curr)
    schemas_list = see_schemas(curr)
    schema_name, table_list = see_tables(curr, schemas_list)
    gdf, table_name, geom_dimension = load_data(schema_name, conn, table_list, curr)
    gdf, all_columns = allclean_columns(gdf)
    table_size_0 = table_size(curr, schema_name, table_name)
    excluded_columns, remain_list = db_action(gdf, all_columns)
    insert_dtypes = construct_sql(gdf, curr, conn, schema_name, table_name, geom_dimension)
    drop_oldDB(schema_name, table_name, curr)
    create_table_cmd(schema_name, table_name, insert_dtypes, curr, conn)
    load_gdf2pg(gdf, engine, table_name, schema_name)
    table_size_1 = table_size(curr, schema_name, table_name)

    writeMeta(db_name, database_size, table_name, table_size_0, table_size_1, excluded_columns, remain_list)

    engine.dispose()
