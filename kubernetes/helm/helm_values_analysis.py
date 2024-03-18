import os
import urllib.parse
from configparser import ConfigParser
from pathlib import Path
from typing import List, AnyStr, Dict

import pandas as pd
import typer
from flatten_dict import flatten, unflatten
from loguru import logger
from ruyaml.main import YAML

import kubernetes.common
from kubernetes.git import git_commands

app = typer.Typer()
yaml = YAML()
yaml.allow_duplicate_keys = True


class HelmValuesAnalysis:
    def __init__(self, values_files_folder: Path):
        self.value_placeholders: List[AnyStr] = []
        self.values_files_folder = values_files_folder
        # list of all values yaml files
        self.value_yaml_files: set[Path] = set()
        self.non_value_yaml_files: set[Path] = set()
        # list of valid (convertible to dict) values yaml docs
        self.value_docs_flat: List[dict] = []
        self.non_value_docs_flat: List[dict] = []
        self.value_file_to_doc_flat: Dict[Path, dict] = {}
        self.value_file_to_properties_dict: Dict[Path, dict] = {}
        self.tags: dict = {}
        # unique key=value pairs so duplicated keys might appear with different value
        self.value_properties: set[str] = set()
        self.unique_value_properties: dict[str, bytes] = dict()
        self.value_flat_dict: dict[tuple, str] = dict()
        self.value_keys: set[tuple] = set()
        self.initialize()

    def initialize(self):
        self.set_value_non_value_files(helm_charts_repo=self.values_files_folder)
        self.load_all_value_files()
        self.set_value_keys()
        self.keys_in_non_value_files()
        self.value_keys = sorted(self.value_keys)
        self.summary_after_init()

    def summary_after_init(self):
        typer.echo(f"value_docs_flat :  {len(self.value_docs_flat)}")
        typer.echo(f"value_yaml_files :  {len(self.value_yaml_files)}")
        typer.echo(f"non_value_yaml_files :  {len(self.non_value_yaml_files)}")

    def load_value_file(self, value_file: Path):
        with open(value_file, 'r') as vf:
            try:
                docs: List = list(yaml.load_all(vf))
                logger.debug(msg=f"Loaded {len(docs)} from {value_file}")
                # self.docs = docs
                for doc in docs:
                    if isinstance(doc, dict):
                        doc_flat = flatten(doc)
                        self.value_docs_flat.append(doc_flat)
                        self.value_file_to_doc_flat[value_file] = doc_flat
                    else:
                        logger.error(msg=f"{value_file.resolve()} contains non dict doc = {doc}")
            except Exception as e:
                print(e)

    def load_all_value_files(self):
        for v_f in self.value_yaml_files:
            self.load_value_file(value_file=v_f)

    def set_value_keys(self):
        for doc_flat in self.value_docs_flat:
            for key in doc_flat.keys():
                self.value_keys.add(key)

    def list_keys(self, contain: str = None):
        """ list keys which contain 'contain'"""
        for doc_flat in self.value_docs_flat:
            for key in doc_flat.keys():
                if contain in key:
                    typer.echo(f"{key}={doc_flat[key]}")

    def set_value_non_value_files(self, helm_charts_repo: Path, filename_contains: str = "values",
                                  file_contains: str = 'resources:'):
        all_yaml_files = set(kubernetes.common.list_files(folder=helm_charts_repo, ends_with=".yaml"))
        yaml_file_contains = set(kubernetes.common.files_containing(files=all_yaml_files, contains=file_contains))
        yaml_file_name_contains = set(kubernetes.common.list_files(folder=helm_charts_repo,
                                                                   ends_with=".yaml", contains=filename_contains))
        yaml_files_union: set = yaml_file_contains.union(yaml_file_name_contains)
        self.value_yaml_files = yaml_files_union
        self.non_value_yaml_files = all_yaml_files - self.value_yaml_files

    def analyze_value_files(self, branch: str, report_folder: Path):
        res_dfs = self.value_files_to_dfs()
        res_dfs.sort(key=lambda x: len(x[0]), reverse=True)
        html_output = ""
        os.makedirs(report_folder, exist_ok=True)
        for df, modified in res_dfs:
            if not df.empty:
                # only one column = last 4 parts incl. filename of the path
                column_tuple = df.columns[0]
                relative_path = Path(*column_tuple)
                header = sizing_header(branch=branch, source_path=str(relative_path), modified=modified)
                html_output += (header + df.to_html())
                json_file = Path(report_folder, f'{branch}_{column_tuple[-1]}.json')
                ini_file = Path(report_folder, f'{branch}_{column_tuple[-1]}.ini')
                df.to_json(json_file, indent=2)
                paths: List[tuple] = sorted(set([key[:key.index('resources')] for key in df.index]))
                config = ConfigParser()
                for path in paths:
                    section = "/".join(path)
                    config[section] = {}
                with open(ini_file, "w") as file:
                    config.write(file)
        html_file_path = Path(report_folder, f"{branch}_resources.html")
        with open(html_file_path, 'w') as file:
            file.write(html_output)

    @staticmethod
    def line_with_values(line: AnyStr) -> bool:
        try:
            line.index('.Values.')
            return True
        except ValueError:
            return False

    def keys_in_non_value_files(self):
        for nvf in sorted(self.non_value_yaml_files):
            with open(nvf, 'r', encoding='utf-8') as f:
                content: List[AnyStr] = f.readlines()
                with_values = [x for x in content if self.line_with_values(x)]
                if len(with_values) > 0:
                    for with_val in with_values:
                        self.value_placeholders.append(with_val)

    @staticmethod
    def doc_to_properties(doc_flat) -> Dict:
        ret = {}
        for key in doc_flat.keys():
            # key is tuple
            property_key = '.'.join(key)
            property_value = str(doc_flat[key]).encode(encoding='utf-8')
            ret[property_key] = property_value
        return ret

    def unique_keys(self):
        ret = []
        for vdf in self.value_docs_flat:
            ret = ret + list(vdf.keys())
        return sorted(set(ret))

    def value_files_to_dfs(self, filter_out: str = 'resources'):
        """
        Returns list of tuples: DataFrame with resources from given values yaml file
        and last modified info of that file
        """
        u_k = self.unique_keys()
        y = {}
        dfs = []
        modified = []
        for file_path, flat_doc in self.value_file_to_doc_flat.items():
            properties = {}
            for key in u_k:
                try:
                    val = flat_doc[key]
                    properties[key] = str(val)
                    logger.debug(f"{key} = {val}")
                except KeyError:
                    pass
            # tuple of str
            parts = file_path.parts
            repo_parts_len = len(self.values_files_folder.parts)
            rel_path_parts = parts[repo_parts_len:]
            rel_path: Path = Path(*rel_path_parts)
            last_modified = git_commands.git_last_file_change(rel_path=rel_path, git_dir=self.values_files_folder)
            modified.append(last_modified)
            resources = {k: v for k, v in properties.items() if filter_out in k}
            y[rel_path_parts] = resources
            new_df = pd.DataFrame(resources, index=[rel_path_parts]).transpose()
            dfs.append(new_df.dropna())
        ret = list(zip(dfs, modified))
        return ret

    def yaml_to_properties(self, properties_file: Path):
        """
        outputs sorted set of all (unique) pairs of keys/properties and values
        found in all values yaml files
        """
        for doc_flat in self.value_docs_flat:
            for key in doc_flat.keys():
                # key is tuple
                property_key = '.'.join(key)
                property_value = str(doc_flat[key]).encode(encoding='utf-8')
                self.value_properties.add(f"{property_key}={property_value}\n")
                self.unique_value_properties[property_key] = property_value
        with open(properties_file, 'w') as outfile:
            typer.echo(f"Writing {len(self.value_properties)} unique key/value pairs")
            outfile.writelines(sorted(self.value_properties))

    def properties_to_yaml(self):
        # self.value_properties convert property key to tuple
        for key, val in sorted(self.unique_value_properties.items()):
            tuple_key = tuple(key.split('.'))
            unquoted = urllib.parse.unquote(str(val), encoding='utf-8')
            decoded_val = None if unquoted == 'None' else unquoted
            # dict() with tuple key and url decoded value
            if decoded_val is not None:
                self.value_flat_dict[tuple_key] = decoded_val
        # unflatten and url decode
        doc = unflatten(self.value_flat_dict)
        with open(f"bin/pykubernetes/helm/properties.yaml", 'w') as outfile:
            yaml.dump(data=doc, stream=outfile)


def sizing_header(branch: str, source_path: str, modified: str) -> str:
    b = f"<li>Branch : {branch}</li>\n"
    p = f"<li>Relative path: {source_path}</li>\n"
    m = f"<li>Last modified: {modified}</li>\n"
    return f"<ul>{b}{p}{m}</ul>\n"


if __name__ == "__main__":
    app()
