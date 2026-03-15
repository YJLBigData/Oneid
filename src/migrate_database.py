import argparse

from config import MySQLConfig
from mysql_cli import fetch_server_tsv, run_server_sql


def quote_ident(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate all base tables from one MySQL schema to another.")
    parser.add_argument("--source-db", default="test", help="Source database name.")
    parser.add_argument("--target-db", default="oneid", help="Target database name.")
    return parser.parse_args()


def load_table_names(source_db: str, cfg: MySQLConfig) -> list[str]:
    sql = (
        "SELECT table_name "
        "FROM information_schema.tables "
        f"WHERE table_schema = '{source_db}' AND table_type = 'BASE TABLE' "
        "ORDER BY table_name"
    )
    return [row[0] for row in fetch_server_tsv(sql, cfg=cfg)]


def migrate_table(table_name: str, source_db: str, target_db: str, cfg: MySQLConfig) -> None:
    source = f"{quote_ident(source_db)}.{quote_ident(table_name)}"
    target = f"{quote_ident(target_db)}.{quote_ident(table_name)}"
    sql = (
        "SET FOREIGN_KEY_CHECKS = 0;"
        f"DROP TABLE IF EXISTS {target};"
        f"CREATE TABLE {target} LIKE {source};"
        f"INSERT INTO {target} SELECT * FROM {source};"
        "SET FOREIGN_KEY_CHECKS = 1;"
    )
    run_server_sql(sql, cfg=cfg)


def main() -> None:
    args = parse_args()
    if args.source_db == args.target_db:
        raise ValueError("source-db and target-db must be different.")

    cfg = MySQLConfig.from_env()
    run_server_sql(
        f"CREATE DATABASE IF NOT EXISTS {quote_ident(args.target_db)} "
        "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci",
        cfg=cfg,
    )

    table_names = load_table_names(args.source_db, cfg=cfg)
    if not table_names:
        raise RuntimeError(f"No base tables found in database `{args.source_db}`.")

    for table_name in table_names:
        migrate_table(table_name, args.source_db, args.target_db, cfg=cfg)

    print(
        f"Migrated {len(table_names)} tables from `{args.source_db}` to `{args.target_db}`: "
        + ", ".join(table_names)
    )


if __name__ == "__main__":
    main()
