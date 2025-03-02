import requests
from pandas import DataFrame, read_csv, to_datetime

def temp() -> DataFrame:
    """Get the temperature data.
    Returns:
        DataFrame: A dataframe with the temperature data.
    """
    return read_csv("data/temperature.csv")

def get_public_holidays(public_holidays_url: str, year: str) -> DataFrame:
    """Get the public holidays for the given year for Brazil.
    Args:
        public_holidays_url (str): URL to the public holidays API.
        year (str): The year to get the public holidays for.
    Raises:
        SystemExit: If the request fails.
    Returns:
        DataFrame: A dataframe with the public holidays.
    """
    # La URL es public_holidays_url/{year}/BR.
    # Debes eliminar las columnas "types" y "counties" del DataFrame.
    # Debes convertir la columna "date" a datetime.
    # Debes lanzar SystemExit si la solicitud falla. Investiga el método raise_for_status
    # de la biblioteca requests.
    url = f"{public_holidays_url}/{year}/BR"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Lanza una excepción si la solicitud falla
        data = response.json()
        
        # Convertir los datos a DataFrame y eliminar columnas no deseadas
        df = DataFrame(data).drop(columns=["types", "counties"], errors='ignore')
        
        # Convertir la columna "date" a datetime
        df["date"] = to_datetime(df["date"])
        
        return df
    except requests.RequestException as e:
        print(f"Error al obtener los días festivos: {e}")
        raise SystemExit(e)

def extract(
    csv_folder: str, csv_table_mapping: dict, public_holidays_url: str
) -> dict:
    """Extract the data from the CSV files and load them into the dataframes.
    Args:
        csv_folder (str): The path to the CSV folder.
        csv_table_mapping (Dict[str, str]): The mapping of the CSV file names to the
        table names.
        public_holidays_url (str): The URL to the public holidays API.
    Returns:
        Dict[str, DataFrame]: A dictionary with keys as the table names and values as
        the dataframes.
    """
    dataframes = {
        table_name: read_csv(f"{csv_folder}/{csv_file}")
        for csv_file, table_name in csv_table_mapping.items()
    }

    holidays = get_public_holidays(public_holidays_url, "2020")
    dataframes["public_holidays"] = holidays
    

    return dataframes
