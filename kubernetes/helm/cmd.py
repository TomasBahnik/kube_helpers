import json
import logging
from pathlib import Path
from typing import List

import typer
from pandas import DataFrame

from cpt import common
from cpt.configuration import COMMON_PROPERTIES
from cpt.helm import kubectl_commands

app = typer.Typer()
logger = logging.getLogger(__name__)
DEFAULT_LOGS_DUMP_FOLDER: Path = Path(COMMON_PROPERTIES.cpt_artefacts_dir, 'logs_dumps')
DEFAULT_LOGS_DUMP: Path = Path(DEFAULT_LOGS_DUMP_FOLDER, f'{common.time_stamp()}.json')


def logs_from_file(file: Path = DEFAULT_LOGS_DUMP) -> dict:
    with open(file=file) as f:
        typer.echo(f'Loading logs from {file.resolve()}')
        logs_data = json.loads(f.read())
        return logs_data


@app.command()
def get_pod_logs(from_file: bool = typer.Option(True, help="Load logs from file or from cluster"),
                 matched_pods: str = typer.Option('dpm', help="Include pods which contains any of this")):
    if from_file:
        logs: dict = logs_from_file()
    else:
        matched_pods = matched_pods.split(",")
        typer.echo(f'Loading logs from cluster for {matched_pods}')
        pods: List[dict] = kubectl_commands.pods()
        pods_dfs: List[DataFrame] = [DataFrame(data=data) for data in pods]
        all_pod_names = [x.loc['name'].loc['metadata'] for x in pods_dfs]
        running_pod_names = [x.loc['name'].loc['metadata'] for x in pods_dfs if
                             x.loc['phase'].loc['status'] == 'Running']
        filter_pod_names = [pod_name for pod_name in running_pod_names if any([m in pod_name for m in matched_pods])]
        logs: dict = kubectl_commands.pod_logs(pod_names=filter_pod_names)
        with open(DEFAULT_LOGS_DUMP, "w") as json_file:
            typer.echo(f'dumping to {DEFAULT_LOGS_DUMP.resolve()}')
            json.dump(logs, json_file, indent=4, sort_keys=True)
    for k, v in logs.items():
        typer.echo(message=f'{k}: log length: {len(v)}')


if __name__ == "__main__":
    app()
