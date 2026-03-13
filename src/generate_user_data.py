import argparse
import random
import string
from collections.abc import Iterator

from mysql_cli import run_sql


DEFAULT_SEED = 20260310
DEFAULT_PERSONS = 4_200
ANCHOR_FIELDS = ["phone", "email", "wx_unionid", "alipay_id", "tmall_id"]
EMAIL_DOMAINS = [
    "oneid-demo.com",
    "identity-lab.net",
    "member-hub.co",
    "shop-graph.io",
]


def sql_literal(value: str | int | None) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, int):
        return str(value)
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def chunked(seq: list[tuple], size: int) -> Iterator[list[tuple]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def random_token(rng: random.Random, length: int) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(rng.choice(alphabet) for _ in range(length))


def build_unique_phone(rng: random.Random, used_phones: set[str]) -> str:
    prefixes = ["130", "131", "132", "155", "156", "166", "177", "186", "188", "199"]
    while True:
        phone = rng.choice(prefixes) + "".join(rng.choice(string.digits) for _ in range(8))
        if phone not in used_phones:
            used_phones.add(phone)
            return phone


def build_person_identity(rng: random.Random, person_no: int, used_phones: set[str]) -> dict[str, str]:
    token = random_token(rng, 10)
    email_local = f"acct.{random_token(rng, 6)}.{person_no:04x}"
    return {
        "user_prefix": f"usr_{token}",
        "phone": build_unique_phone(rng, used_phones),
        "email": f"{email_local}@{rng.choice(EMAIL_DOMAINS)}",
        "wx_unionid": f"wun_{random_token(rng, 18)}",
        "alipay_id": f"ali_{random_token(rng, 16)}",
        "tmall_id": f"tm_{random_token(rng, 14)}",
    }


def choose_account_count(rng: random.Random) -> int:
    return rng.choices([3, 4, 5, 6], weights=[12, 43, 31, 14], k=1)[0]


def choose_anchor_fields(
    rng: random.Random,
    used_fields: set[str],
    all_fields: list[str],
    is_first_account: bool,
    remaining_accounts: int,
) -> list[str]:
    if is_first_account:
        selected = set(rng.sample(all_fields, k=rng.randint(2, 3)))
    else:
        selected = {rng.choice(sorted(used_fields))}

    unused_fields = [field for field in all_fields if field not in used_fields and field not in selected]
    if remaining_accounts == 1:
        selected.update(unused_fields)
    elif unused_fields:
        new_count = rng.randint(1, min(2, len(unused_fields)))
        selected.update(rng.sample(unused_fields, k=new_count))

    reusable_fields = [field for field in all_fields if field not in selected]
    if reusable_fields and rng.random() < 0.45:
        selected.add(rng.choice(reusable_fields))

    used_fields.update(selected)
    return sorted(selected)


def build_rows(
    persons: int = DEFAULT_PERSONS,
    seed: int = DEFAULT_SEED,
) -> list[tuple[str, str | None, str | None, str | None, str, str | None, str | None]]:
    rng = random.Random(seed)
    used_phones: set[str] = set()
    rows: list[tuple[str, str | None, str | None, str | None, str, str | None, str | None]] = []

    for person_no in range(1, persons + 1):
        person_identity = build_person_identity(rng, person_no, used_phones)
        account_count = choose_account_count(rng)
        used_anchor_fields: set[str] = set()

        for account_idx in range(account_count):
            user_id = f"{person_identity['user_prefix']}_{account_idx}_{random_token(rng, 6)}"
            wx_openid = f"wxo_{random_token(rng, 22)}"
            selected_fields = choose_anchor_fields(
                rng=rng,
                used_fields=used_anchor_fields,
                all_fields=ANCHOR_FIELDS,
                is_first_account=account_idx == 0,
                remaining_accounts=account_count - account_idx,
            )

            row_dict: dict[str, str | None] = {field: None for field in ANCHOR_FIELDS}
            for field in selected_fields:
                row_dict[field] = person_identity[field]

            rows.append(
                (
                    user_id,
                    row_dict["phone"],
                    row_dict["email"],
                    row_dict["wx_unionid"],
                    wx_openid,
                    row_dict["alipay_id"],
                    row_dict["tmall_id"],
                )
            )

    return rows


def insert_rows(rows: list[tuple], batch_size: int = 500) -> None:
    run_sql("TRUNCATE TABLE `user`")
    columns = "(`user_id`, `phone`, `email`, `wx_unionid`, `wx_openid`, `alipay_id`, `tmall_id`)"

    for batch in chunked(rows, batch_size):
        values_sql = []
        for row in batch:
            values_sql.append("(" + ", ".join(sql_literal(v) for v in row) + ")")
        sql = f"INSERT INTO `user` {columns} VALUES {', '.join(values_sql)}"
        run_sql(sql)


def generate_and_load(
    persons: int = DEFAULT_PERSONS,
    seed: int = DEFAULT_SEED,
    batch_size: int = 500,
) -> int:
    rows = build_rows(persons=persons, seed=seed)
    insert_rows(rows, batch_size=batch_size)
    print(
        "Inserted "
        f"{len(rows)} rows into table `user` with random seed {seed} and {persons} natural persons."
    )
    return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate randomized OneID demo data and load it into MySQL.")
    parser.add_argument("--persons", type=int, default=DEFAULT_PERSONS, help="Number of natural persons to generate.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Random seed for reproducible data.")
    parser.add_argument("--batch-size", type=int, default=500, help="MySQL insert batch size.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    generate_and_load(persons=args.persons, seed=args.seed, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
