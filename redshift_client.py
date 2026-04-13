import configparser
import os
import redshift_connector
import pandas as pd
from pathlib import Path


def get_config(config_path=None):
    if config_path is None:
        config_path = Path(__file__).parent / "redshift_config.ini"
    config = configparser.ConfigParser()
    config.read(config_path)
    return config["redshift"]


def get_connection(config_path=None):
    # Use environment variables if available (Railway), else fall back to config file
    host = os.environ.get("REDSHIFT_HOST", "").strip()
    if host:
        return redshift_connector.connect(
            host=host,
            port=int(os.environ.get("REDSHIFT_PORT", "5439").strip()),
            database=os.environ.get("REDSHIFT_DB", "dwh").strip(),
            user=os.environ.get("REDSHIFT_USER", "").strip(),
            password=os.environ.get("REDSHIFT_PASSWORD", "").strip(),
        )
    else:
        cfg = get_config(config_path)
        return redshift_connector.connect(
            host=cfg["host"],
            port=int(cfg["port"]),
            database=cfg["database"],
            user=cfg["user"],
            password=cfg["password"],
        )


def run_query(query, config_path=None):
    conn = get_connection(config_path)
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()


if __name__ == "__main__":
    df = run_query("SELECT 1 AS test")
    print(df)
