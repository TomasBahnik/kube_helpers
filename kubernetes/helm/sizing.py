import json
import logging
from pathlib import Path
from typing import List

import dpath
import pandas as pd
from jinja2 import Template
from ruyaml.main import YAML

import kubernetes.logging
from kubernetes.helm.common import resource_value

CONTAINERS_KEY = "spec/template/spec/containers"

yaml = YAML()
yaml.preserve_quotes = True
#  https://yaml.readthedocs.io/en/latest/api.html#duplicate-keys
yaml.allow_duplicate_keys = True
logger = logging.getLogger(__name__)


class Sizing:
    """ Creates report from manifest """
    def __init__(self, manifest_file: Path):
        self.logger = logging.getLogger(kubernetes.logging.fullname(self))
        self.interpolated_yaml = manifest_file
        self.all_docs: List = []
        self.container_docs = []
        self.kind_docs = []
        self.load_docs()

    def load_docs(self):
        with open(self.interpolated_yaml, 'r') as yaml_file:
            self.all_docs = list(yaml.load_all(yaml_file))
        self.container_docs = docs_with_key(self.all_docs, key=CONTAINERS_KEY)
        self.kind_docs = docs_with_key(self.all_docs, key='kind')

    def extract_resources(self):
        resources_dict = {}
        for doc in self.container_docs:
            self.logger.debug("Processing doc kind : {}".format(doc["kind"]))
            replicas = dpath.get(obj=doc, glob="spec/replicas", default=None)
            container = dpath.get(obj=doc, glob=CONTAINERS_KEY)[0]
            # spec.template.spec.containers[0].env
            env = dpath.get(obj=container, glob="env", default=None)
            name = dpath.get(obj=container, glob="name", default=None)
            resources = dpath.get(obj=container, glob="resources", default=None)
            image = dpath.get(obj=container, glob="image", default=None)
            if name and resources:
                resources.update({'replicas': replicas})
                resources.update({'image': image})
                resources.update({'env': env})
                # runtime type of resources is ruyaml.comments.CommentedMap (ordered dict)
                resources_dict[name] = resolve_empty_requests(dict(resources))
            else:
                # can't explicitly access keys limits and requests - leads to unclear html template
                self.logger.debug("Add empty resources for {}".format(name))
                resources_dict[name] = {'limits': {}, 'requests': {}, 'replicas': {}}
        return resources_dict

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

    def save_sizing(self, base_file_name='sizing', save_path: Path = None):
        output_json = Path(save_path, f'{base_file_name}.json')
        output_html = Path(save_path, f'{base_file_name}.html')
        output_csv = Path(save_path, f'{base_file_name}.csv')
        output_props = Path(save_path, f'{base_file_name}_properties.json')
        metrics = self.extract_resources()
        volumes = self.extract_volumes()
        properties = self.extract_properties()
        metrics.update({'volumes': volumes})
        self.logger.info(f"Create sizing from {self.interpolated_yaml} to {output_json}")
        with open(output_json, "w") as json_file:
            json.dump(metrics, json_file, indent=4, sort_keys=True)
        with open(output_props, "w") as props_file:
            json.dump(properties, props_file, indent=4, sort_keys=True)
        # remove key for html/csv
        metrics.pop('volumes', None)
        html = template_html.render(module_resources=sorted(metrics.items(), key=lambda item: item[0]))
        with open(output_html, "w") as html_file:
            html_file.write(html)
        n_m = normalize_metrics(metrics)  # overwrites original. Use deep copy if problem
        csv = template_csv.render(module_resources=sorted(n_m.items(), key=lambda item: item[0]))
        with open(output_csv, "w") as csv_file:
            csv_file.write(csv)
        return metrics


template_html = Template('''
    <!DOCTYPE html>
    <html>
    <head>
    <style>
    table, th, td {
        border: 1px solid black;
    }
    </style>
    </head>
    <body>
    <h2>Resources</h2>
    <table border="1" cellpadding="5">
        <tr><th>module</th><th colspan="2">limits</th><th colspan="2">requests</th></tr>
        <tr><th></th><th>memory</th><th>cpu</th><th>memory</th><th>cpu</th><th>replicas</th><th>image</th></tr>
        {% for module, resources in module_resources %}
        <tr>
            <td>{{module}}</td>
            <td>{{resources['limits']['memory']}}</td><td>{{resources['limits']['cpu']}}</td>
            <td>{{resources['requests']['memory']}}</td><td>{{resources['requests']['cpu']}}</td><td>{{resources['replicas']}}</td><td>{{resources['image']}}</td>
        </tr>            
        {% endfor %}
    </table>
    </body>
    </html> 
    ''')
template_csv = Template('''module,memory_limits,cpu_limits,memory_requests,cpu_requests,replicas
{% for module, resources in module_resources %}{{module}},{{resources['limits']['memory']}},{{resources['limits']['cpu']}},{{resources['requests']['memory']}},{{resources['requests']['cpu']}},{{resources['replicas']}}
{% endfor %}
''')


def get_resources(resources, key, sub_key):
    if key in resources and sub_key in resources[key]:
        return resources[key][sub_key]
    else:
        return None


# original metrics are replaces by floats - use for csv
def normalize_metrics(metrics):
    for module, resources in metrics.items():
        l_mem = get_resources(resources, 'limits', 'memory')
        l_cpu = get_resources(resources, 'limits', 'cpu')
        r_mem = get_resources(resources, 'requests', 'memory')
        r_cpu = get_resources(resources, 'requests', 'cpu')
        logger.debug("get resource values for module {}".format(module))
        metrics[module]['limits']['memory'] = resource_value(l_mem)
        metrics[module]['limits']['cpu'] = resource_value(l_cpu)
        metrics[module]['requests']['memory'] = resource_value(r_mem)
        metrics[module]['requests']['cpu'] = resource_value(r_cpu)
    return metrics


def compare_resources(r1, r2):
    df1 = pd.read_csv(r1)
    df2 = pd.read_csv(r2)
    return pd.merge(df1, df2, on=['module'], how='outer', suffixes=('_helm', '_kust'))


def containers(docs):
    return docs_with_key(docs, key=CONTAINERS_KEY)


def docs_with_key(docs, key: str):
    ret: List[dict] = []
    for doc in docs:
        v = dpath.get(doc, key, default=None)
        if v:
            ret.append(doc)
        else:
            continue
    return ret


def resolve_empty_requests(metric: dict):
    v = dpath.get(metric, 'requests', default=None)
    if not v:
        dpath.new(metric, 'requests', {'cpu': None, 'memory': None})
    return metric
