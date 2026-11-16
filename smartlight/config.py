import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    database: Path = Path("data/smartlight.db")
    mqtt_host: str = "127.0.0.1"
    mqtt_port: int = 1883
    origins: tuple[str, ...] = ("http://localhost:5173", "http://127.0.0.1:5173")
    queue_size: int = 1024

    @classmethod
    def from_env(cls):
        return cls(
            database=Path(os.getenv("SMARTLIGHT_DB", "data/smartlight.db")),
            mqtt_host=os.getenv("SMARTLIGHT_MQTT_HOST", "127.0.0.1"),
            mqtt_port=int(os.getenv("SMARTLIGHT_MQTT_PORT", "1883")),
            origins=tuple(
                x.strip() for x in os.getenv("SMARTLIGHT_ORIGINS", ",".join(cls.origins)).split(",") if x.strip()
            ),
        )
