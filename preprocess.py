import datetime
import functools
import json
import logging
import os
from collections import defaultdict
import operator

import coiled
import deltalake
import fsspec
import pandas as pd
import toolz
from dask.distributed import LocalCluster, print, wait
from upath import UPath as Path

logger = logging.getLogger(__name__)

LOCAL = False
if LOCAL:
    OUTDIR = Path(f"./data")
    STORAGE_OPTIONS = {}
    Cluster = LocalCluster
else:
    import boto3
    OUTDIR = Path(f"s3://openscapes-scratch/etl-github")

    session = boto3.session.Session()
    credentials = session.get_credentials()

    STORAGE_OPTIONS = dict(
        AWS_SECRET_ACCESS_KEY=credentials.secret_key,
        AWS_ACCESS_KEY_ID=credentials.access_key,
        AWS_SESSION_TOKEN=credentials.token,
        AWS_REGION="us-west-2",
        AWS_S3_ALLOW_UNSAFE_RENAME="true",
    )
    Cluster = functools.partial(
        coiled.Cluster,
        name="etl-github-matt",
        region="us-west-2",
        shutdown_on_close=False,
    )


def handle_PushEvent(d):
    for commit in d["payload"]["commits"]:
        yield {
            "username": d["actor"]["login"],
            "repo": d["repo"]["name"],
            "sha": commit["sha"],
            "message": commit["message"],
            "created_at": datetime.datetime.fromisoformat(d["created_at"]),
        }


def handle_CreateEvent(d):
    return {
        "username": d["actor"]["login"],
        "repo": d["repo"]["name"],
        "type": d["payload"]["ref_type"],
        "name": d["payload"]["ref"],
        "description": d["payload"]["description"],
        "created_at": datetime.datetime.fromisoformat(d["created_at"]),
    }


def handle_PullRequestEvent(d):
    return {
        "username": d["actor"]["login"],
        "repo": d["repo"]["name"],
        "action": d["payload"]["action"],
        "number": d["payload"]["number"],
        "title": d["payload"]["pull_request"]["title"],
        "author": d["payload"]["pull_request"]["user"]["login"],
        "body": d["payload"]["pull_request"]["body"],
        "pr_created_at": datetime.datetime.fromisoformat(
            d["payload"]["pull_request"]["created_at"]
        ),
        "created_at": datetime.datetime.fromisoformat(d["created_at"]),
    }


def handle_IssueCommentEvent(d):
    return {
        "username": d["actor"]["login"],
        "repo": d["repo"]["name"],
        "number": d["payload"]["issue"]["number"],
        "title": d["payload"]["issue"]["title"],
        "author": d["payload"]["issue"]["user"]["login"],
        "issue_created_at": datetime.datetime.fromisoformat(
            d["payload"]["issue"]["created_at"]
        ),
        "comment": d["payload"]["comment"]["body"],
        "association": d["payload"]["comment"]["author_association"],
        "created_at": datetime.datetime.fromisoformat(d["created_at"]),
    }


def handle_WatchEvent(d):
    return {
        "username": d["actor"]["login"],
        "repo": d["repo"]["name"],
        "action": d["payload"]["action"],
        "created_at": datetime.datetime.fromisoformat(d["created_at"]),
    }


def handle_ForkEvent(d):
    return {
        "username": d["actor"]["login"],
        "repo": d["repo"]["name"],
        "created_at": datetime.datetime.fromisoformat(d["created_at"]),
    }



conversions = {
    "PushEvent": handle_PushEvent,
    "CreateEvent": handle_CreateEvent,
    "PullRequestEvent": handle_PullRequestEvent,
    "IssueCommentEvent": handle_IssueCommentEvent,
    "WatchEvent": handle_WatchEvent,
    "ForkEvent": handle_ForkEvent,
}


def process_records(seq):
    out = defaultdict(list)
    for record in seq:
        try:
            record = json.loads(record)
        except Exception:
            continue

        if record["type"] in conversions:
            convert = conversions[record["type"]]
            try:
                result = convert(record)
            except Exception as e:
                logger.error(f"Failed to parse record: {e}")
                continue

            out[record["type"]].append(result)

    return out


def process_file(filename: str) -> dict[str, pd.DataFrame]:
    with fsspec.open(
        filename, compression="gzip" if filename.endswith("gz") else None
    ) as f:
        out = process_records(f)
        out["Commits"] = list(toolz.concat(out["PushEvent"]))

    del out["PushEvent"]
    out = {k: pd.DataFrame(v) for k, v in out.items()}
    out = {
        "comment": out["IssueCommentEvent"],
        "pr": out["PullRequestEvent"],
        "commit": out["Commits"],
        "create": out["CreateEvent"],
        "watch": out["WatchEvent"],
        "fork": out["ForkEvent"],
    }
    for df in out.values():
        df["date"] = df.created_at.dt.date
    return out


def write_delta(df: pd.DataFrame, name: str):
    outfile = OUTDIR / name
    outfile.fs.makedirs(outfile.parent, exist_ok=True)
    deltalake.write_deltalake(
        outfile,
        df,
        mode="append",
        storage_options=STORAGE_OPTIONS,
        partition_by="date",
    )

def list_files(start, stop):
    dates = pd.date_range(start, stop - datetime.timedelta(days=1), freq="d")
    hours = [str(dt.date()) + f"-{h}.json.gz" for dt in dates for h in range(24)]
    filenames = ["https://data.gharchive.org/" + hour for hour in hours]
    return filenames


def compact(table):
    t = deltalake.DeltaTable(
        OUTDIR / table,
        storage_options=STORAGE_OPTIONS,
    )
    t.optimize.compact()
    t.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
    print(f"Compacted {table} table")


if __name__ == "__main__":
    filenames = list_files(
        start=datetime.date(2023, 1, 1),
        stop=datetime.date(2023, 1, 2),
    )
    tables = ["comment", "pr", "commit", "create", "watch", "fork"]

    with Cluster() as cluster:
        with cluster.get_client() as client:
            client.restart()
            futures = client.map(process_file, filenames)
            all_writes = []
            for name in tables:
                writes = client.map(lambda a, b: a[b], futures, b=name)
                writes = client.map(write_delta, writes, name=name)
                all_writes.extend(writes)
            del futures
            wait(all_writes)

            futures = client.map(compact, tables)
            wait(futures)
