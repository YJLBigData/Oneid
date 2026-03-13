from collections import defaultdict
from collections.abc import Iterator

from mysql_cli import fetch_tsv, run_sql


IDENTITY_FIELDS = ["phone", "email", "wx_unionid", "wx_openid", "alipay_id", "tmall_id"]


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}
        self.rank: dict[str, int] = {}

    def add(self, x: str) -> None:
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x: str) -> str:
        parent = self.parent[x]
        if parent != x:
            self.parent[x] = self.find(parent)
        return self.parent[x]

    def union(self, a: str, b: str) -> None:
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            self.parent[ra] = rb
        elif self.rank[ra] > self.rank[rb]:
            self.parent[rb] = ra
        else:
            self.parent[rb] = ra
            self.rank[ra] += 1


def normalize(v: str) -> str | None:
    if v in {"", "NULL", "\\N"}:
        return None
    return v


def chunked(seq: list[tuple[str, str]], size: int) -> Iterator[list[tuple[str, str]]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def sql_literal(value: str | int | None) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, int):
        return str(value)
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def load_user_rows() -> list[list[str]]:
    sql = """
    SELECT user_id, phone, email, wx_unionid, wx_openid, alipay_id, tmall_id
    FROM `user`
    ORDER BY user_id
    """
    return fetch_tsv(sql)


def build_oneid_mapping(rows: list[list[str]]) -> list[tuple[str, str]]:
    uf = UnionFind()
    user_nodes: list[tuple[str, str]] = []

    for row in rows:
        user_id = row[0]
        user_node = f"user:{user_id}"
        uf.add(user_node)
        user_nodes.append((user_node, user_id))

        for field, raw_val in zip(IDENTITY_FIELDS, row[1:]):
            value = normalize(raw_val)
            if not value:
                continue
            id_node = f"{field}:{value}"
            uf.add(id_node)
            uf.union(user_node, id_node)

    root_to_users: dict[str, list[str]] = defaultdict(list)
    for user_node, user_id in user_nodes:
        root_to_users[uf.find(user_node)].append(user_id)

    grouped_users = sorted((sorted(users) for users in root_to_users.values()), key=lambda x: x[0])

    result: list[tuple[str, str]] = []
    for idx, users in enumerate(grouped_users, start=1):
        oneid = f"ONEID{idx:08d}"
        for user_id in users:
            result.append((oneid, user_id))
    return result


def persist_oneid_result(result_rows: list[tuple[str, str]], batch_size: int = 1000) -> None:
    run_sql("TRUNCATE TABLE `oneid_result`")
    for batch in chunked(result_rows, batch_size):
        values = ", ".join(
            f"({sql_literal(oneid)}, {sql_literal(user_id)})" for oneid, user_id in batch
        )
        run_sql(f"INSERT INTO `oneid_result` (`oneid`, `user_id`) VALUES {values}")


def main() -> None:
    rows = load_user_rows()
    if not rows:
        raise RuntimeError("Table `user` is empty. Please run data generation first.")

    result_rows = build_oneid_mapping(rows)
    persist_oneid_result(result_rows)

    unique_oneid = len({oneid for oneid, _ in result_rows})
    print(f"OneID resolved: {len(result_rows)} user links, {unique_oneid} unique oneid.")


if __name__ == "__main__":
    main()
