import json
import os
from decimal import Decimal
from typing import Any, Optional
import datetime

from snowflake import connector
from snowflake.connector import cursor
from snowflake.core import exceptions

type Response = dict[str, Any]


class NotFoundError(Exception):
    def __init__(self, resource):
        msg = f"No content found at: {resource}"
        super().__init__(msg)


class SnowflakeJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        else:
            super().default(o)


class Snowflake:
    def __init__(self):
        # TODO: handle other connection methods
        account = os.getenv("SNOWFLAKE_ACCOUNT")
        assert account, "SNOWFLAKE_ACCOUNT environment variable must be set"

        user = os.getenv("SNOWFLAKE_USER")
        assert user, "SNOWFLAKE_USER environment variable must be set"

        role = os.getenv("SNOWFLAKE_ROLE")
        assert role, "SNOWFLAKE_ROLE environment variable must be set"

        key_file = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE")
        assert key_file, "SNOWFLAKE_PRIVATE_KEY_FILE environment variable must be set"

        self.conn = connector.connect(
            account=account,
            user=user,
            role=role,
            private_key_file=key_file,
        )

    def cur(self):
        return self.conn.cursor()

    def sanitised(self, value: Any):
        match value:
            case datetime.date() | datetime.datetime():
                return value.isoformat()
            case _:
                return value

    def execute_query(
        self,
        query: str,
        params: Optional[tuple[str]] = None,
        outfile: Optional[str] = None,
        cur: Optional[cursor.SnowflakeCursor] = None,
    ) -> Response:
        if cur is None:
            cur = self.cur()

        assert cur

        res = []
        try:
            # TODO: handle list of tuples of params
            res = cur.execute(query, params).fetchall()
        except (
            exceptions.UnauthorizedError,
            exceptions.NotFoundError,
            exceptions.ForbiddenError,
        ) as e:
            return {"error": e.reason, "status": e.status}
        except Exception as e:
            return {"error": str(e)}

        if len(res) == 0:
            raise NotFoundError(query)

        headers = [x[0] for x in cur.description]

        if outfile:
            dir = os.path.dirname(outfile)
            file = os.path.basename(outfile)
            os.makedirs(dir, exist_ok=True)
            with open(os.path.join(dir, file), mode="w") as f:
                for row in res:
                    json.dump(
                        {field: value for field, value in zip(headers, row)},
                        f,
                        cls=SnowflakeJsonEncoder,
                    )
                    f.write("\n")

            return {"status": "success", "file": outfile}
        else:
            return {
                "status": "success",
                "data": [
                    {field: self.sanitised(value) for field, value in zip(headers, row)}
                    for row in res
                ],
            }

    def execute_query_batch(
        self,
        queries: list[str],
        return_n: int = -1,
    ) -> Response:
        cur = self.cur()

        res = []
        for query in queries:
            res.append(self.execute_query(query, cur=cur))

        return res[return_n]
