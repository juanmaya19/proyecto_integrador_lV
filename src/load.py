from typing import Dict
from pandas import DataFrame
from sqlalchemy.engine.base import Engine

def load(data_frames: Dict[str, DataFrame], database: Engine):
    """Load the dataframes into the SQLite database.

    Args:
        data_frames (Dict[str, DataFrame]): A dictionary with keys as the table names
        and values as the dataframes.
        database (Engine): SQLAlchemy database engine to connect to the database.
    """
    for table_name, df in data_frames.items():
        try:
            df.to_sql(name=table_name, con=database, if_exists="replace", index=False)
            print(f"✅ Tabla '{table_name}' cargada con éxito en la base de datos.")
        except Exception as e:
            print(f"❌ Error al cargar la tabla '{table_name}': {e}")

