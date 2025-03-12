import pandas as pd
from pytest import fixture
from src.config import QUERY_RESULTS_ROOT_PATH, DATASET_ROOT_PATH, PUBLIC_HOLIDAYS_URL
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import json
import math
from src.transform import (
    query_delivery_date_difference,
    query_global_ammount_order_status,
    query_revenue_by_month_year,
    query_revenue_per_state,
    query_top_10_least_revenue_categories,
    query_top_10_revenue_categories,
    query_real_vs_estimated_delivered_time,
    query_orders_per_day_and_holidays_2017,
    query_freight_value_weight_relationship,
)
from src.load import load
from src.extract import extract
from src.config import get_csv_to_table_mapping
from src.transform import QueryResult

TOLERANCE = 0.1


def to_float(objs, year_col):
    return list(map(lambda obj: float(obj[year_col]) if obj[year_col] else 0.0, objs))


def float_vectors_are_close(a: list, b: list, tolerance: float = TOLERANCE) -> bool:
    """Check if two vectors of floats are close.
    Args:
        a (list): The first vector.
        b (list): The second vector.
        tolerance (float): The tolerance.
    Returns:
        bool: True if the vectors are close, False otherwise.
    """
    return all([math.isclose(a[i], b[i], abs_tol=tolerance) for i in range(len(a))])


@fixture(scope="session", autouse=True)
def database() -> Engine:
    """Initialize the database for testing."""
    engine = create_engine("sqlite://")
    csv_folder = DATASET_ROOT_PATH
    public_holidays_url = PUBLIC_HOLIDAYS_URL
    csv_table_mapping = get_csv_to_table_mapping()
    csv_dataframes = extract(csv_folder, csv_table_mapping, public_holidays_url)
    load(data_frames=csv_dataframes, database=engine)
    return engine


def read_query_result(query_name: str) -> dict:
    """Read the query from the json file.
    Args:
        query_name (str): The name of the query.
    Returns:
        dict: The query as a dictionary.
    """
    with open(f"{QUERY_RESULTS_ROOT_PATH}/{query_name}.json", "r") as f:
        query_result = json.load(f)

    return query_result


def pandas_to_json_object(df: pd.DataFrame) -> dict:
    """Convert pandas dataframe to json object.
    Args:
        df (pd.DataFrame): The dataframe.
    Returns:
        dict: The dataframe as a json object.
    """
    return json.loads(df.to_json(orient="records"))




def test_query_revenue_by_month_year(database: Engine):
    query_name = "revenue_by_month_year"
    actual = pandas_to_json_object(query_revenue_by_month_year(database).result)
    expected = read_query_result(query_name)

    def normalize_data(objs, year_col):
        return [
            float(str(obj[year_col]).replace(",", ".")) if obj[year_col] else 0.0
            for obj in objs
        ]

    assert len(actual) == len(expected), f"Diferente cantidad de filas: {len(actual)} vs {len(expected)}"
    assert [obj["month_no"] for obj in actual] == [obj["month_no"] for obj in expected], "Los meses no coinciden"

    for year in ["Year2016", "Year2017", "Year2018"]:
        actual_values = normalize_data(actual, year)
        expected_values = normalize_data(expected, year)
        
        differences = [
            (a, e) for a, e in zip(actual_values, expected_values) if not math.isclose(a, e, rel_tol=1e-3)
        ]
        
        assert not differences, f"Diferencias en {year}: {differences}"

    assert list(actual[0].keys()) == list(expected[0].keys()), "Las columnas no coinciden"




def test_query_delivery_date_difference(database: Engine):
    query_name = "delivery_date_difference"
    actual: QueryResult = query_delivery_date_difference(database)
    expected = read_query_result(query_name)
    assert pandas_to_json_object(actual.result) == expected


def test_query_global_ammount_order_status(database: Engine):
    query_name = "global_ammount_order_status"
    actual: QueryResult = query_global_ammount_order_status(database)
    expected = read_query_result(query_name)
    assert pandas_to_json_object(actual.result) == expected


def test_query_revenue_per_state(database: Engine):
    query_name = "revenue_per_state"
    actual = pandas_to_json_object(query_revenue_per_state(database).result)
    expected = read_query_result(query_name)
    assert len(actual) == len(expected)
    assert list(actual[0].keys()) == list(expected[0].keys())
    assert float_vectors_are_close(
        [obj["Revenue"] for obj in actual], [obj["Revenue"] for obj in expected]
    )


def test_query_top_10_least_revenue_categories(database: Engine):
    query_name = "top_10_least_revenue_categories"
    actual = pandas_to_json_object(
        query_top_10_least_revenue_categories(database).result
    )
    expected = read_query_result(query_name)
    assert len(actual) == len(expected)
    assert list(actual[0].keys()) == list(expected[0].keys())
    assert [obj["Category"] for obj in actual] == [obj["Category"] for obj in expected]
    assert [obj["Num_order"] for obj in actual] == [
        obj["Num_order"] for obj in expected
    ]
    assert float_vectors_are_close(
        [obj["Revenue"] for obj in actual], [obj["Revenue"] for obj in expected]
    )


def test_query_top_10_revenue_categories(database: Engine):
    query_name = "top_10_revenue_categories"
    actual = pandas_to_json_object(query_top_10_revenue_categories(database).result)
    expected = read_query_result(query_name)

    # Verificar longitud
    assert len(actual) == len(expected)

    # Verificar claves en los objetos
    assert list(actual[0].keys()) == list(expected[0].keys())

    # Verificar que las categorías coincidan exactamente
    assert [obj["Category"] for obj in actual] == [obj["Category"] for obj in expected]

    # Verificar que los valores de "Num_order" sean los mismos
    assert [obj["Num_order"] for obj in actual] == [obj["Num_order"] for obj in expected]

    # Verificar que los valores de "Revenue" sean cercanos con tolerancia
    assert all(
        math.isclose(a, b, rel_tol=1e-5)  # Permite pequeñas diferencias por precisión flotante
        for a, b in zip([obj["Revenue"] for obj in actual], [obj["Revenue"] for obj in expected])
    )

def test_real_vs_estimated_delivered_time(database: Engine):
    query_name = "real_vs_estimated_delivered_time"
    actual = pandas_to_json_object(
        query_real_vs_estimated_delivered_time(database).result
    )
    expected = read_query_result(query_name)

    def to_float(objs, year_col):
        return list(
            map(lambda obj: float(obj[year_col]) if obj[year_col] else 0.0, objs)
        )

    assert len(actual) == len(expected)
    assert list(actual[0].keys()) == list(expected[0].keys())
    assert [obj["month_no"] for obj in actual] == [obj["month_no"] for obj in expected]
    assert float_vectors_are_close(
        to_float(actual, "Year2016_real_time"), to_float(expected, "Year2016_real_time")
    )
    assert float_vectors_are_close(
        to_float(actual, "Year2017_real_time"), to_float(expected, "Year2017_real_time")
    )
    assert float_vectors_are_close(
        to_float(actual, "Year2018_real_time"), to_float(expected, "Year2018_real_time")
    )
    assert float_vectors_are_close(
        to_float(actual, "Year2016_estimated_time"),
        to_float(expected, "Year2016_estimated_time"),
    )
    assert float_vectors_are_close(
        to_float(actual, "Year2017_estimated_time"),
        to_float(expected, "Year2017_estimated_time"),
    )
    assert float_vectors_are_close(
        to_float(actual, "Year2018_estimated_time"),
        to_float(expected, "Year2018_estimated_time"),
    )


def test_query_orders_per_day_and_holidays_2017(database: Engine):
    query_name = "orders_per_day_and_holidays_2017"
    actual: QueryResult = query_orders_per_day_and_holidays_2017(database)
    expected = read_query_result(query_name)
    assert pandas_to_json_object(actual.result) == expected


def test_query_get_freight_value_weight_relationship(database: Engine):
    """Obtiene los datos de la relación entre valor del flete y peso total y los muestra en consola."""
    
    actual: QueryResult = query_freight_value_weight_relationship(database)
    
    # Convertir el resultado a JSON
    actual_json = pandas_to_json_object(actual.result)
    
    # Normalizar datos redondeando los valores flotantes a 2 decimales
    for item in actual_json:
        item["total_freight_value"] = round(item["total_freight_value"], 2)
        item["total_weight_g"] = round(item["total_weight_g"], 2)

    # Imprimir solo 50 registros para no sobrecargar la salida
    print(actual_json[:50])
