import datetime
import functools
import json
import logging
import os
from collections import defaultdict

import coiled
import deltalake
import fsspec
import pandas as pd
import toolz
from dask.distributed import LocalCluster, print, wait
from prefect import flow
from upath import UPath as Path

logger = logging.getLogger(__name__)

LOCAL = True
if LOCAL:
    OUTDIR = Path(f"./data")
    STORAGE_OPTIONS = {}
    Cluster = LocalCluster
else:
    OUTDIR = Path(f"s3://openscapes-scratch/etl-github-test")
    STORAGE_OPTIONS = {"AWS_REGION": "us-west-2", "AWS_S3_ALLOW_UNSAFE_RENAME": "true"}
    Cluster = functools.partial(
        coiled.Cluster,
        name="etl-github",
        n_workers=50,
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
    out = toolz.valmap(pd.DataFrame, out)
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


def write_delta(tables: dict[str, pd.DataFrame]):
    for table, df in tables.items():
        outfile = OUTDIR / table
        outfile.fs.makedirs(outfile.parent, exist_ok=True)
        deltalake.write_deltalake(
            outfile,
            df,
            mode="append",
            storage_options=STORAGE_OPTIONS,
            partition_by="date",
        )

def list_files(start, stop):
    dts = pd.date_range(start, stop, freq="1h")
    filenames = [f"https://data.gharchive.org/{dt.date()}-{dt.hour}.json.gz" for dt in dts]
    return filenames


def compact(table):
    t = deltalake.DeltaTable(
        OUTDIR / table,
        storage_options=STORAGE_OPTIONS,
    )
    t.optimize.compact()
    t.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
    print(f"Compacted {table} table")


def parse_start_stop(start, stop):
    """ If no provided, determine start and stop times """
    if start is None:
        t = deltalake.DeltaTable(
            OUTDIR / "comment",
            storage_options=STORAGE_OPTIONS,
        )
        actions = t.get_add_actions(flatten=True).to_pandas()
        start = actions["max.created_at"].max().ceil("1h")
    if stop is None:
        # Current hour won't be fully populated yet, so subtract an hour
        stop = pd.Timestamp.now().floor("1h") - datetime.timedelta(hours=1)
    return start, stop


@flow(log_prints=True)
def workflow(start=None, stop=None):
    start, stop = parse_start_stop(start, stop)
    filenames = list_files(start, stop)
    print(f"{filenames = }")
    with Cluster() as cluster:
        if not LOCAL:
            cluster.send_private_envs({
                "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
                "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
            })
        with cluster.get_client() as client:
            client.restart()
            futures = client.map(process_file, filenames)
            futures = client.map(write_delta, futures, retries=10)
            wait(futures)
            tables = ["comment", "pr", "commit", "create", "watch", "fork"]
            futures = client.map(compact, tables)
            wait(futures)


if __name__ == "__main__":

    # Initial bulk processing of historical data
    workflow(
        start=datetime.datetime(2024, 2, 29, hour=0),
        stop=datetime.datetime(2024, 3, 1, hour=3),
    )
    # Process new data hourly
    workflow.serve(
        name="github-processing",
        interval=datetime.timedelta(hours=1),
    )