"""Read the lakehouse SQL files so Spark jobs can run them statement by statement.

Kept free of any Spark import so it can be unit-tested without a Spark installation.
"""

from pathlib import Path

DEFAULT_BRONZE = "local.bronze"
DEFAULT_SILVER = "local.silver"
DEFAULT_GOLD = "local.gold"


def split_statements(sql: str) -> list[str]:
    """Split a SQL script on semicolons after dropping '--' comment lines.

    The script format forbids semicolons inside strings and comments, so a plain split is
    enough. A trailing statement without a semicolon is kept.
    """
    lines = [line for line in sql.splitlines() if not line.lstrip().startswith("--")]
    statements = (part.strip() for part in "\n".join(lines).split(";"))
    return [statement for statement in statements if statement]


def with_namespaces(
    sql: str, bronze: str = DEFAULT_BRONZE, silver: str = DEFAULT_SILVER, gold: str = DEFAULT_GOLD
) -> str:
    """Point a script at different namespaces, for example throwaway ones in a test."""
    return sql.replace(DEFAULT_BRONZE, bronze).replace(DEFAULT_SILVER, silver).replace(DEFAULT_GOLD, gold)


def load_statements(
    path: str | Path,
    bronze: str = DEFAULT_BRONZE,
    silver: str = DEFAULT_SILVER,
    gold: str = DEFAULT_GOLD,
) -> list[str]:
    text = Path(path).read_text(encoding="utf-8")
    return split_statements(with_namespaces(text, bronze, silver, gold))
