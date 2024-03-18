from pathlib import Path
from typing import List

from kubernetes.common import time_stamp
from kubernetes.configuration import COMMON_PROPERTIES
from kubernetes.os.cmd_line import run_cmd

HELM_MAIN_COMMAND = 'helm'
HELM_NS = 'product'
HELM_APP_NAME = 'ataccama-one'
HELM_CHARTS_PATH: Path = COMMON_PROPERTIES.charts_path


def uninstall(helm_app_name: str = HELM_APP_NAME, helm_ns: str = HELM_NS) -> List[str]:
    return [HELM_MAIN_COMMAND, 'uninstall', helm_app_name] + ['-n', helm_ns]


def deploy(command: str, helm_app_name: str, helm_ns: str, dry_run: bool):
    """ command = install or upgrade"""
    cmd = [HELM_MAIN_COMMAND, command, helm_app_name, str(HELM_CHARTS_PATH)] + ['-n', helm_ns]
    if dry_run:
        cmd += ['--dry-run']
    return cmd


def install(helm_app_name: str, helm_ns: str, dry_run: bool) -> List[str]:
    return deploy(command='install', helm_app_name=helm_app_name, helm_ns=helm_ns, dry_run=dry_run)


def upgrade(helm_app_name: str, helm_ns: str, dry_run: bool) -> List[str]:
    return deploy(command='upgrade', helm_app_name=helm_app_name, helm_ns=helm_ns, dry_run=dry_run)


def update_dependency() -> List[str]:
    return [HELM_MAIN_COMMAND, 'dependency', 'update', str(HELM_CHARTS_PATH)]


def get_notes(helm_app_name: str = HELM_APP_NAME, helm_ns: str = HELM_NS) -> str:
    # NOTES:
    # deploymentInfo: test_env=paas_dq_sa,branch=release-14.3.X,sizing=perf_standard,modules=basic_ai
    cmd = [HELM_MAIN_COMMAND, 'get', 'notes'] + [helm_app_name] + ['-n', helm_ns]
    std_out, std_err = run_cmd(cmd=cmd)
    return std_out


def get_values(helm_app_name: str = HELM_APP_NAME, helm_ns: str = HELM_NS) -> str:
    cmd = [HELM_MAIN_COMMAND, 'get', 'values'] + [helm_app_name] + ['-n', helm_ns]
    std_out, std_err = run_cmd(cmd=cmd)
    return std_out if std_out else std_err


def get(command: str, helm_app_name: str, helm_ns: str, folder: Path = None, file_name: str = None) -> str | Path:
    """Get values, notes, manifest. std out is stored to the folder, if specified
    and path is returned
    """
    cmd = [HELM_MAIN_COMMAND, 'get', command] + [helm_app_name] + ['-n', helm_ns]
    std_out, std_err = run_cmd(cmd=cmd)
    if folder:
        file_name: str = file_name if file_name else f'{helm_ns}_{command}_{time_stamp()}.yaml'
        file_path: Path = Path(folder, file_name)
        with open(file_path, 'w', encoding='utf-8') as yaml_file:
            yaml_file.write(std_out)
        return file_path
    return std_out if std_out else std_err
