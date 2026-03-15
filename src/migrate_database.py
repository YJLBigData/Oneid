import argparse

from config import MySQLConfig
from mysql_cli import fetch_server_tsv, run_server_sql

PROJECT_TABLES = ("user", "oneid_result")


def quote_ident(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate OneID project tables from one MySQL schema to another.")
    parser.add_argument("--source-db", default="test", help="Source database name.")
    parser.add_argument("--target-db", default="oneid", help="Target database name.")
    parser.add_argument(
        "--tables",
        default=",".join(PROJECT_TABLES),
        help="Comma separated table list. Defaults to OneID project tables only.",
    )
    return parser.parse_args()


def load_table_names(source_db: str, cfg: MySQLConfig, table_names: list[str]) -> list[str]:
    quoted_names = ", ".join(f"'{table_name}'" for table_name in table_names)
    sql = (
        "SELECT table_name "
        "FROM information_schema.tables "
        f"WHERE table_schema = '{source_db}' AND table_type = 'BASE TABLE' "
        f"AND table_name IN ({quoted_names}) "
        "ORDER BY table_name"
    )
    return [row[0] for row in fetch_server_tsv(sql, cfg=cfg)]


def load_extra_target_tables(target_db: str, cfg: MySQLConfig, keep_tables: list[str]) -> list[str]:
    quoted_names = ", ".join(f"'{table_name}'" for table_name in keep_tables)
    sql = (
        "SELECT table_name "
        "FROM information_schema.tables "
        f"WHERE table_schema = '{target_db}' AND table_type = 'BASE TABLE' "
        f"AND table_name NOT IN ({quoted_names}) "
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


def cleanup_target_tables(target_db: str, keep_tables: list[str], cfg: MySQLConfig) -> list[str]:
    extra_tables = load_extra_target_tables(target_db, cfg=cfg, keep_tables=keep_tables)
    if not extra_tables:
        return []

    quoted_tables = ", ".join(f"{quote_ident(target_db)}.{quote_ident(table_name)}" for table_name in extra_tables)
    run_server_sql(f"SET FOREIGN_KEY_CHECKS = 0; DROP TABLE IF EXISTS {quoted_tables}; SET FOREIGN_KEY_CHECKS = 1;", cfg=cfg)
    return extra_tables


def main() -> None:
    args = parse_args()
    if args.source_db == args.target_db:
        raise ValueError("source-db and target-db must be different.")

    selected_tables = [table_name.strip() for table_name in args.tables.split(",") if table_name.strip()]
    if not selected_tables:
        raise ValueError("No tables selected for migration.")

    cfg = MySQLConfig.from_env()
    run_server_sql(
        f"CREATE DATABASE IF NOT EXISTS {quote_ident(args.target_db)} "
        "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci",
        cfg=cfg,
    )

    table_names = load_table_names(args.source_db, cfg=cfg, table_names=selected_tables)
    if not table_names:
        raise RuntimeError(f"No matching tables found in database `{args.source_db}`.")

    missing_tables = sorted(set(selected_tables) - set(table_names))
    if missing_tables:
        raise RuntimeError(
            "Missing source tables in database "
            f"`{args.source_db}`: {', '.join(missing_tables)}"
        )

    for table_name in table_names:
        migrate_table(table_name, args.source_db, args.target_db, cfg=cfg)

    dropped_tables = cleanup_target_tables(args.target_db, keep_tables=table_names, cfg=cfg)

    print(
        f"Migrated {len(table_names)} tables from `{args.source_db}` to `{args.target_db}`: "
        + ", ".join(table_names)
    )
    if dropped_tables:
        print(
            f"Removed {len(dropped_tables)} unrelated tables from `{args.target_db}`: "
            + ", ".join(dropped_tables)
        )
    else:
        print(f"No unrelated tables found in `{args.target_db}`.")


if __name__ == "__main__":
    main()
