from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from tqdm import tqdm

from mirror_temperature.settings import Settings


def save(output_folder: Path, data: pd.DataFrame, table: str) -> None:
    """Ramples DESI telemetry data contained within Pandas dataframe.

    Parameters
    ----------
    data : pandas.DataFrame
        Queried data from the DESI telemetry database

    table : str
        Table name of data to save

    Returns
    -------
    None
    """
    data.to_pickle(output_folder / f"{table}.pkl")


def load(rows: np.ndarray, columns: list[str]) -> pd.DataFrame:
    """Preprocesses DESI telemetry data contained within Pandas dataframe.

    Parameters
    ----------
    rows : numpy.ndarray
        Unprocessed data from the DESI telemetry database

    columns : list[str]
        column names that were queired from database

    Returns
    -------
    data : pandas.DataFrame
        Resampled and interpolated DESI telemetry data
    """

    # Note: not entirely clear _why_ we are making some of these transformations,
    # but it is likely required to save into FITS format later.
    data = (
        pd.DataFrame(rows, columns=columns)
        .sort_values("time_recorded")
        .replace({pd.NA: np.nan})
        .rename(columns={"time_recorded": "time"})
        .assign(time=lambda df: df.time.dt.strftime("%Y-%m-%dT%H:%M:%S.%f").astype(str))
    )
    return data


def _query_environmentmonitor_tower(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    "time_recorded",
                    "temperature",
                    "pressure",
                    "humidity",
                    "wind_speed",
                    "wind_direction"
                FROM
                    "environmentmonitor_tower"
                LIMIT 10
                """
                # WHERE time_recorded < X AND time_recorded > y
            )
            rows = cur.fetchall()
            data = load(rows=rows, columns=[
                "time_recorded",
                "temperature",
                "pressure",
                "humidity",
                "wind_speed",
                "wind_direction",
            ])
    return data


def _query_environmentmonitor_dome(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    "time_recorded",
                    "dome_left_upper",
                    "dome_left_lower",
                    "dome_right_upper",
                    "dome_right_lower",
                    "dome_back_upper",
                    "dome_back_lower",
                    "dome_floor_ne",
                    "dome_floor_nw",
                    "dome_floor_s"
                FROM
                    "environmentmonitor_dome"
                LIMIT 10
                """
                # WHERE time_recorded < X AND time_recorded > y
            )
            rows = cur.fetchall()
            data = load(rows=rows, columns=[
                "time_recorded",
                "dome_left_upper",
                "dome_left_lower",
                "dome_right_upper",
                "dome_right_lower",
                "dome_back_upper",
                "dome_back_lower",
                "dome_floor_ne",
                "dome_floor_nw",
                "dome_floor_s",
            ])
    return data


def _query_environmentmonitor_telescope(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    "time_recorded",
                    "mirror_avg_temp",
                    "mirror_desired_temp",
                    "mirror_temp",
                    "mirror_cooling",
                    "air_temp",
                    "air_flow",
                    "air_dewpoint"
                FROM
                    "environmentmonitor_telescope"
                LIMIT 10
                """
                # WHERE time_recorded < X AND time_recorded > y
            )
            rows = cur.fetchall()
            data = load(rows=rows, columns=[
                "time_recorded",
                "mirror_avg_temp",
                "mirror_desired_temp",
                "mirror_temp",
                "mirror_cooling",
                "air_temp",
                "air_flow",
                "air_dewpoint",
            ])
    return data


def _query_etc_seeing(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    "time_recorded",
                    "etc_seeing",
                    "seeing"
                FROM
                    "etc_seeing"
                LIMIT 10
                """
                # WHERE time_recorded < X AND time_recorded > y
            )
            rows = cur.fetchall()
            data = load(rows=rows, columns=[
                "time_recorded",
                "etc_seeing",
                "seeing",
            ])
    return data


def _query_etc_telemetry(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    "time_recorded",
                    "seeing",
                    "transparency",
                    "skylevel"
                FROM
                    "etc_telemetry"
                LIMIT 10
                """
                # WHERE time_recorded < X AND time_recorded > y
            )
            rows = cur.fetchall()
            data = load(rows=rows, columns=[
                "time_recorded",
                "seeing",
                "transparency",
                "skylevel",
            ])
    return data


def _query_tcs_info(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    "time_recorded",
                    "mirror_ready",
                    "airmass"
                FROM
                    "tcs_info"
                LIMIT 10
                """
                # WHERE time_recorded < X AND time_recorded > y
            )
            rows = cur.fetchall()
            data = load(rows=rows, columns=[
                "time_recorded",
                "mirror_ready",
                "airmass",
            ])
    return data


def retrieve_and_store_data_from_database(settings: Settings) -> None:
    """Main function to facilitate queries of DESI telemetry data

    Parameters
    ----------
    None

    Returns
    -------
    None
    """

## FIRST (messier version) OF QUERYING
 # Data table names and respective columns of interest
 #   labels["environmentmonitor_tower"] = [
 #       "time_recorded",
 #       "temperature",
 #       "pressure",
 #       "humidity",
 #       "wind_speed",
 #       "wind_direction",
 #   labels["environmentmonitor_dome"] = [
 #       "time_recorded",
 #       "dome_left_upper",
 #       "dome_left_lower",
 #       "dome_right_upper",
 #       "dome_right_lower",
 #       "dome_back_upper",
 #       "dome_back_lower",
 #       "dome_floor_ne",
 #       "dome_floor_nw",
 #       "dome_floor_s",
 #   labels["environmentmonitor_telescope"] = [
 #       "time_recorded",
 #       "mirror_avg_temp",
 #       "mirror_desired_temp",
 #       "mirror_temp",
 #       "mirror_cooling",
 #       "air_temp",
 #       "air_flow",
 #       "air_dewpoint",

    # Establish connection to server and begin SQL queries using labels shown above.
    pd.DataFrame()
    conn = psycopg2.connect(
        host=settings.desi_db_host,
        port=settings.desi_db_port,
        database=settings.desi_db_database,
        user=settings.desi_db_username,
        password=settings.desi_db_password,
    )

    table_queries = {
        "test_environmentmonitor_tower": _query_environmentmonitor_tower,
        "test_environmentmonitor_dome": _query_environmentmonitor_dome,
        "test_environmentmonitor_telescope": _query_environmentmonitor_telescope,
        "test_etc_seeing": _query_etc_seeing,
        "test_etc_telemetry": _query_etc_telemetry,
        "test_tcs_info": _query_tcs_info,
    }

    for table_name, query in tqdm(table_queries.items(), desc="hello"):
        rv = query(conn)
        save(settings.output_folder, rv, table=table_name)

    #for table, columns in tqdm(labels.items(), desc="Querying database tables..."):
     #   with conn:
      #      with conn.cursor() as cur:
       #         # TODO: Selecting this way is bad practice.
        #        # Write explicit selects for each table without any cleverness.
