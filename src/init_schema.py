from pathlib import Path

from mysql_cli import run_sql_file


BASE_DIR = Path(__file__).resolve().parent.parent


def main() -> None:
    sql_file = BASE_DIR / "sql" / "01_create_tables.sql"
    run_sql_file(sql_file)
    print(f"Schema initialized: {sql_file}")


if __name__ == "__main__":
    main()
