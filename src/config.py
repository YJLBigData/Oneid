import os
from dataclasses import dataclass


@dataclass
class MySQLConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    socket: str | None = None

    @classmethod
    def from_env(cls) -> "MySQLConfig":
        socket = os.getenv("MYSQL_SOCKET", "").strip() or None
        return cls(
            host=os.getenv("MYSQL_HOST", "localhost"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", "toor"),
            database=os.getenv("MYSQL_DB", "test"),
            socket=socket,
        )

    def mysql_args(self) -> list[str]:
        args = ["mysql", f"-u{self.user}", f"-p{self.password}", self.database]
        if self.socket:
            args[1:1] = [f"--socket={self.socket}"]
        else:
            args[1:1] = ["--protocol=TCP", f"-h{self.host}", f"-P{self.port}"]
        return args
