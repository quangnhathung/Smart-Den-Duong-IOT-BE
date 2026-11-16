"""Initialize/version-check SQLite without deleting data or connecting MQTT."""

import argparse
from pathlib import Path

from .storage import DatabaseLock, open_database


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("data/smartlight.db"))
    args = parser.parse_args()
    lock = DatabaseLock(args.db)
    try:
        db = open_database(args.db)
        try:
            print(f"Initialized schema v{db.execute('PRAGMA user_version').fetchone()[0]}: {args.db}")
        finally:
            db.close()
    finally:
        lock.close()


if __name__ == "__main__":
    main()
