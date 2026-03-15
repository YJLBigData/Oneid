from pathlib import Path

from config import MySQLConfig
from mysql_cli import run_server_sql, run_sql_file


BASE_DIR = Path(__file__).resolve().parent.parent


def main() -> None:
    cfg = MySQLConfig.from_env()
    run_server_sql(
        f"CREATE DATABASE IF NOT EXISTS `{cfg.database}` "
        "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci",
        cfg=cfg,
    )
    sql_file = BASE_DIR / "sql" / "01_create_tables.sql"
    run_sql_file(sql_file, cfg=cfg)
    print(f"Schema initialized in database `{cfg.database}`: {sql_file}")


if __name__ == "__main__":
    main()
