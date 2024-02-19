import json
import logging
from typing import List, Union

import typer

from kubernetes.helm.helm_commands import get_notes
from kubernetes.os.cmd_line import RunCommand, run_cmd
from kubernetes.prometheus.const import PRODUCT_NS

JSON_OUTPUT = ['-o', 'json']
PRODUCT_NS_OPTION = ['-n', PRODUCT_NS]
DEFAULT_SINCE_OPTION = ['--since', '4h']

logger = logging.getLogger(__name__)


def current_context():
    cmd = ['kubectl', 'config', 'current-context']
    cc, err = run_cmd(cmd=cmd)
    return cc


def use_context(context: str):
    cmd = ['kubectl', 'config', 'use-context', context]
    run_cmd(cmd=cmd)


def pods(namespace: str = PRODUCT_NS) -> List[dict]:
    ns = ['-n', namespace]
    cmd = ['kubectl', 'get', 'pod'] + ns + JSON_OUTPUT
    r_c: RunCommand = RunCommand(command=cmd)
    stdout: str = r_c.stdout_decoded
    data = json.loads(stdout)
    return data['items']


def pod_logs(pod_names: List[str]):
    ret_dict = {}
    for pod_name in pod_names:
        cmd = ['kubectl', 'logs', pod_name] + PRODUCT_NS_OPTION + DEFAULT_SINCE_OPTION
        r_c: RunCommand = RunCommand(command=cmd)
        stdout: str = r_c.stdout_decoded
        ret_dict[pod_name] = stdout
    return ret_dict


def deployment_info(kube_context: str, test_env: str):
    if kube_context is not None:
        use_context(kube_context)
        cc = current_context()
        if test_env.startswith(cc):
            return _deployment_info(notes=get_notes())
        else:
            return f"{test_env} test env does not contains kube context {cc}"
    else:
        return f"{test_env} does not specify kube context"


def _deployment_info(notes: str) -> Union[str, None]:
    try:
        lst = list(map(str.strip, notes.split(':')))
        idx = lst.index('deploymentInfo')
        return lst[idx + 1]
    except ValueError:
        logger.warning(f"deploymentInfo not found in {notes}")
    return None


def resize_minio_pvc(ns_option: List[str]):
    # kubectl get sts --selector='app=minio' -o name -n staging-62znh returns statefulset.apps/minio
    del_all_minio_sts(ns_option=ns_option)
    get_pvc_names(ns_option=ns_option)


def del_all_minio_sts(ns_option: List[str]):
    sts = 'statefulset.apps/minio'
    del_sts_cmd: List[str] = ['kubectl', 'delete', sts] + ns_option + JSON_OUTPUT
    del_sts_run_cmd: RunCommand = RunCommand(command=del_sts_cmd)
    typer.echo(del_sts_run_cmd.error_msg())


def get_pvc_names(ns_option: List[str]) -> List[str]:
    selector = "--selector=app=minio"
    # kubectl get pvc -o name --selector='app=minio' -n staging-62znh
    all_pvc_cmd = ['kubectl', 'get', 'pvc', '-o', 'name', selector] + ns_option + JSON_OUTPUT
    # std = run_cmd(all_pvc_cmd)
    all_pvc_run_cmd: RunCommand = RunCommand(command=all_pvc_cmd)
    stdout: str = all_pvc_run_cmd.stdout_decoded
    data = json.loads(stdout)
    pvcs = data['items']
    pvc_names = [f'persistentvolumeclaim/{item["metadata"]["name"]}' for item in pvcs
                 if item['kind'] == 'PersistentVolumeClaim']
    return pvc_names


def patch_pvcs(pvc_names, ns_option: List[str], value):
    """ value in format as 12Gi"""
    patch_path = f"""'{{"spec":{{"resources": {{"requests": {{"storage":"{value}"}}}}}}}}'"""
    for p_n in pvc_names:
        typer.echo(f'Patch {p_n}')
        cmd = ['kubectl', 'patch', p_n, '-p', patch_path] + ns_option
        r_c: RunCommand = RunCommand(command=cmd)
        typer.echo(r_c.error_msg())


if __name__ == "__main__":
    namespace = 'staging-1h6d2'
    ns_pvc_names = get_pvc_names(ns_option=['-n', namespace])
    typer.echo(f'{namespace}: {ns_pvc_names}')
