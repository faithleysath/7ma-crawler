from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from urllib.parse import SplitResult, urlsplit, urlunsplit

import psycopg
import pytest
from psycopg import sql


def _replace_database(url: str, database_name: str) -> str:
    parts = urlsplit(url)
    path = f"/{database_name}"
    rebuilt = SplitResult(
        scheme=parts.scheme,
        netloc=parts.netloc,
        path=path,
        query=parts.query,
        fragment=parts.fragment,
    )
    return urlunsplit(rebuilt)


@pytest.fixture
def test_database_url() -> Iterator[str]:
    base_url = os.getenv("TEST_DATABASE_URL")
    if not base_url:
        pytest.skip("TEST_DATABASE_URL is not set; skipping PostgreSQL integration test.")

    database_name = f"sevenma_test_{uuid.uuid4().hex[:12]}"
    temporary_url = _replace_database(base_url, database_name)

    with psycopg.connect(base_url, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("create database {}").format(sql.Identifier(database_name))
            )

    try:
        yield temporary_url
    finally:
        with psycopg.connect(base_url, autocommit=True) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    select pg_terminate_backend(pid)
                    from pg_stat_activity
                    where datname = %s and pid <> pg_backend_pid()
                    """,
                    (database_name,),
                )
                cursor.execute(
                    sql.SQL("drop database if exists {}").format(sql.Identifier(database_name))
                )
