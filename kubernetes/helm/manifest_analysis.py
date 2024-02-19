import logging
import os
from collections import defaultdict
from enum import StrEnum
from pathlib import Path
from typing import List

import dpath
import pandas as pd
import typer
import yaml

import cpt.logging
from cpt.configuration import COMMON_PROPERTIES
from cpt.prometheus.const import GIBS

NAME_KEY = 'name'

RESOURCES_KEY = 'resources'

LIMITS = 'limits'
REQUESTS = 'requests'

POD_INIT_CONTAINERS_KEY = "spec/initContainers"
POD_CONTAINERS_KEY = "spec/containers"

CONTAINERS_KEY = f"spec/template/{POD_CONTAINERS_KEY}"
INIT_CONTAINERS_KEY = f"spec/template/{POD_INIT_CONTAINERS_KEY}"
# only in Deployment
REPLICAS_PATH = "spec/replicas"

logger = logging.getLogger(__name__)


class ManifestTypes(StrEnum):
    POD = 'pod'
    DEPLOY = 'deploy'
    JOB = 'job'
    MANIFEST = 'manifest'


class ManifestAnalysis:
    def __init__(self, manifest: Path, manifest_type: str):
        self.logger = logging.getLogger(cpt.logging.fullname(self))
        self.manifest = manifest
        self.manifest_type = manifest_type
        self.all_docs: List[dict] = self.load_docs()
        self.kind_docs: List[dict] = [doc for doc in self.all_docs if 'kind' in doc.keys()]
        self.pods: List[dict] = [doc for doc in self.kind_docs if self.kind_docs if doc['kind'] == 'Pod']
        self.deployments = [doc for doc in self.kind_docs if self.kind_docs if doc['kind'] == 'Deployment']
        self.jobs = [doc for doc in self.kind_docs if self.kind_docs if doc['kind'] == 'Job']
        self.replicas = [dpath.get(doc, REPLICAS_PATH, default={}) for doc in self.all_docs]
        # 1 = names, 2 = kinds
        self.containers = self.get_containers()[0]
        self.non_empty_containers = [c for c in self.containers if c]
        self.resources = self.get_resources(source=self.non_empty_containers)
        self.init_containers = self.get_init_containers()
        self.non_empty_init_containers = [c for c in self.init_containers if c]
        self.init_resources = self.get_resources(source=self.non_empty_init_containers)

    def load_docs(self):
        with open(self.manifest, 'r') as yaml_file:
            all_docs = list(yaml.safe_load_all(yaml_file))
            if self.manifest_type == ManifestTypes.MANIFEST:
                return all_docs
            else:
                return all_docs[0]['items']

    def get_containers(self):
        if self.manifest_type == ManifestTypes.POD:
            return [dpath.get(doc, POD_CONTAINERS_KEY, default={}) for doc in self.all_docs]
        else:
            # add doc['metadata']['name'] and doc['kind]
            containers = [dpath.get(doc, CONTAINERS_KEY, default={}) for doc in self.all_docs]
            doc_names = [dpath.get(doc, 'metadata/name', default={}) for doc in self.all_docs]
            doc_kinds = [dpath.get(doc, 'kind', default={}) for doc in self.all_docs]
            return [containers, doc_names, doc_kinds]

    def get_init_containers(self):
        if self.manifest_type == ManifestTypes.POD:
            return [dpath.get(doc, POD_INIT_CONTAINERS_KEY, default={}) for doc in self.all_docs]
        else:
            return [dpath.get(doc, INIT_CONTAINERS_KEY, default={}) for doc in self.all_docs]

    @staticmethod
    def get_resources(source):
        ret: List[dict] = []
        for containers in source:
            for container in containers:
                d = {NAME_KEY: container[NAME_KEY], RESOURCES_KEY: dpath.get(container, RESOURCES_KEY, default={})}
                ret.append(d)
        return ret

    def extract_resources(self, linkerd: bool):
        container_resources = self.resources + self.init_resources
        normalized_resources: List[dict] = [
            {NAME_KEY: c_r[NAME_KEY], RESOURCES_KEY: normalize_metrics(c_r[RESOURCES_KEY])}
            for c_r in container_resources]
        if not linkerd:
            names = [normalized_resource[NAME_KEY] for normalized_resource in normalized_resources
                     if 'linkerd' not in normalized_resource[NAME_KEY]]
            normalized_resources = [normalized_resource[RESOURCES_KEY] for normalized_resource in normalized_resources
                                    if 'linkerd' not in normalized_resource[NAME_KEY]]
        else:
            names = [normalized_resource[NAME_KEY] for normalized_resource in normalized_resources]
            normalized_resources = [normalized_resource[RESOURCES_KEY] for normalized_resource in normalized_resources]
        return {NAME_KEY: names, RESOURCES_KEY: normalized_resources}

    def extract_volumes(self):
        resources_dict = {}
        for doc in self.kind_docs:
            self.logger.debug("Processing doc kind : {}".format(doc["kind"]))
            volumes = dpath.get(obj=doc, glob="spec/volumeClaimTemplates", default=None)
            service_name = dpath.get(obj=doc, glob="spec/serviceName", default=None)
            if volumes and len(volumes) == 1:
                resources = dpath.get(obj=volumes[0], glob="spec/resources/requests/storage")
                resources_dict[service_name] = resources
        return resources_dict

    def extract_properties(self):
        ret = {}
        cms = [d for d in self.kind_docs if d['kind'] == 'ConfigMap']
        for cm in cms:
            name = dpath.get(cm, glob='metadata/name', default=None)
            props = dpath.get(cm, glob='data/application.properties', default=None)
            ret[name] = props
        return ret


def get_resources(resources, key, sub_key):
    if key in resources and sub_key in resources[key]:
        return resources[key][sub_key]
    else:
        return None


def normalize_metrics(orig_resources: dict, multiply_cpu: float = 1, multiply_mem: float = 1) -> dict:
    from cpt.helm.common import resource_value
    normalized_resources = defaultdict(dict)  # solves issue with using missing keys
    l_mem = get_resources(orig_resources, LIMITS, 'memory')
    l_cpu = get_resources(orig_resources, LIMITS, 'cpu')
    r_mem = get_resources(orig_resources, REQUESTS, 'memory')
    r_cpu = get_resources(orig_resources, REQUESTS, 'cpu')
    if REQUESTS in orig_resources.keys():
        normalized_resources[REQUESTS]['memory'] = round(resource_value(r_mem) * multiply_mem, 0)
        normalized_resources[REQUESTS]['cpu'] = round(resource_value(r_cpu) * multiply_cpu, 1)
    normalized_resources[LIMITS]['memory'] = round(resource_value(l_mem) * multiply_mem, 0)
    normalized_resources[LIMITS]['cpu'] = round(resource_value(l_cpu) * multiply_cpu, 1)
    return dict(normalized_resources)


app = typer.Typer()


def totals(resources, res_type: str):
    """
    Totals of resource type
    :param resources:
    :param res_type: either limits or requests
    :return:
    """
    cpu: float = 0
    mem: float = 0
    for resource in resources:
        try:
            values = resource[res_type]
            cpu += values['cpu']
            mem += values['memory']
        except KeyError:
            typer.echo(f'totals: No {res_type}')
    return {'items': len(resources), f'total_{res_type}_cpu': round(cpu, 1),
            f'total_{res_type}_mem [Gi]': round(mem / GIBS, 1)}


@app.command()
def resources(file: Path = typer.Option(..., help='Path of yaml file', dir_okay=True),
              manifest_type: str = typer.Option(ManifestTypes.MANIFEST.value,
                                                help=f'{ManifestTypes.POD.value}, {ManifestTypes.JOB.value}, '
                                                     f'{ManifestTypes.DEPLOY.value}, '
                                                     f'{ManifestTypes.MANIFEST.value}'),
              linkerd: bool = typer.Option(True, help='Include linkerd pods')):
    manifest_analysis = ManifestAnalysis(manifest=file, manifest_type=manifest_type)
    data = manifest_analysis.extract_resources(linkerd=linkerd)
    resources_df: pd.DataFrame = pd.DataFrame(data=data)
    # if not linkerd:
    #     resources_df = resources_df[~resources_df[NAME_KEY].str.contains('linkerd')]
    resources_df.set_index(NAME_KEY, inplace=True)
    resources_df.sort_index(inplace=True)
    folder: Path = Path(*file.parts[:-1])
    filename: str = file.parts[-1]
    bare_filename = filename.removesuffix('.yaml')
    resources_df.to_html(Path(folder, f'{bare_filename}_{manifest_type}_linkerd_{linkerd}.html'))
    typer.echo(f'resources_df: {resources_df.shape}, saved to : {folder}')
    typer.echo(totals(data[RESOURCES_KEY], res_type=LIMITS))
    typer.echo(totals(data[RESOURCES_KEY], res_type=REQUESTS))
    if manifest_type == ManifestTypes.MANIFEST.value:
        from kubernetes.helm.sizing import Sizing
        sizing = Sizing(manifest_file=Path(folder, filename))
        sizing.save_sizing(save_path=folder, base_file_name=f'{bare_filename}_sizing')


@app.command()
def helm_get(command: str = typer.Option(..., "-c", help='values, notes, manifest'),
             namespace: str = typer.Option(..., "-n", help='deployment namespace')):
    from kubernetes.helm import helm_commands
    folder = Path(COMMON_PROPERTIES.helm_perf_values_dir, 'helm_get')
    os.makedirs(folder, exist_ok=True)
    typer.echo(f'folder: {folder}')
    helm_commands.get(command=command, helm_app_name=namespace, helm_ns=namespace, folder=folder)


if __name__ == "__main__":
    app()
