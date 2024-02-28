import json, gzip, toolz, fsspec
import deltalake
from collections import defaultdict
import datetime
import pandas as pd
import dask.distributed
from dask.distributed import as_completed, LocalCluster


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


conversions = {
    "PushEvent": handle_PushEvent,
    "CreateEvent": handle_CreateEvent,
    "PullRequestEvent": handle_PullRequestEvent,
    "IssueCommentEvent": handle_IssueCommentEvent,
}


def process_records(seq):
    out = defaultdict(list)
    for record in seq:
        try:
            record = json.loads(record)
        except Exception:
            continue

        if record["type"] in conversions:
            result = conversions[record["type"]](record)
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
    }
    return out


def write_delta(tables: dict[str, pd.DataFrame]):
    for k, df in result.items():
        deltalake.write_deltalake("data/" + k, df, mode="append")


def list_files(start, stop):
    dates = pd.date_range(start, stop - datetime.timedelta(days=1), freq="d")
    hours = [str(dt.date()) + f"-{h}.json.gz" for dt in dates for h in range(24)]
    filenames = ["https://data.gharchive.org/" + hour for hour in hours]
    return filenames


if __name__ == "__main__":
    filenames = list_files(
        start=datetime.date(2024, 2, 1),
        stop=datetime.date(2024, 2, 3),
    )

    with LocalCluster() as cluster:
        with cluster.get_client() as client:
            futures = client.map(process_file, filenames)
            for future, result in as_completed(futures, with_results=True):
                print(".", end="")
                future.cancel()
                write_delta(result)
