import subprocess
from pathlib import Path

from config import MySQLConfig


class MySQLCommandError(RuntimeError):
    pass


def _run_mysql(args: list[str], sql: str | None = None, stdin_file: Path | None = None) -> subprocess.CompletedProcess[str]:
    if stdin_file:
        with stdin_file.open("r", encoding="utf-8") as f:
            completed = subprocess.run(
                args,
                stdin=f,
                capture_output=True,
                text=True,
                check=False,
            )
    else:
        completed = subprocess.run(
            args,
            input=sql,
            capture_output=True,
            text=True,
            check=False,
        )
    if completed.returncode != 0:
        raise MySQLCommandError(
            "MySQL command failed.\n"
            f"Command: {' '.join(args)}\n"
            f"STDOUT: {completed.stdout}\n"
            f"STDERR: {completed.stderr}"
        )
    return completed


def run_sql(sql: str, cfg: MySQLConfig | None = None) -> None:
    cfg = cfg or MySQLConfig.from_env()
    args = cfg.mysql_args() + ["--default-character-set=utf8mb4", "-e", sql]
    _run_mysql(args)


def run_sql_file(sql_file: Path, cfg: MySQLConfig | None = None) -> None:
    cfg = cfg or MySQLConfig.from_env()
    args = cfg.mysql_args() + ["--default-character-set=utf8mb4"]
    _run_mysql(args, stdin_file=sql_file)


def fetch_tsv(sql: str, cfg: MySQLConfig | None = None) -> list[list[str]]:
    cfg = cfg or MySQLConfig.from_env()
    args = cfg.mysql_args() + [
        "--default-character-set=utf8mb4",
        "--batch",
        "--raw",
        "--skip-column-names",
        "-e",
        sql,
    ]
    completed = _run_mysql(args)
    rows: list[list[str]] = []
    for line in completed.stdout.splitlines():
        rows.append(line.split("\t"))
    return rows
