from app import database as db

def create_serial_sequence(schema_name, table_name, column_name='id'):
    start_id = db.engine.execute('SELECT MAX({}) FROM "{}"."{}"'.format(column_name, schema_name, table_name)).fetchone()[0] + 1
    db.engine.execute("""
    DROP SEQUENCE IF EXISTS "{0}"."{1}_{2}_seq";
    CREATE SEQUENCE IF NOT EXISTS "{0}"."{1}_{2}_seq" START WITH {3}
    OWNED BY "{0}"."{1}".{2};
    ALTER TABLE "{0}"."{1}"
    ALTER {2}
      SET DEFAULT nextval('"{0}"."{1}_{2}_seq"'::regclass);
    """.format(schema_name, table_name, column_name, start_id))

