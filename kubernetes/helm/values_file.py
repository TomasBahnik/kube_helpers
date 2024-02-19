import configparser
import json
import logging
import os
from collections import defaultdict
from enum import Enum
from pathlib import Path
from typing import List, Callable, Any, Union, Optional, Dict, Tuple

import dpath
import pandas as pd
import typer
import yaml
from ruyaml.comments import CommentedMap as OrderedDict
from ruyaml.comments import CommentedSeq as OrderedList
from ruyaml.main import round_trip_load as yaml_load, round_trip_dump as yaml_dump
from ruyaml.scalarstring import LiteralScalarString

import kubernetes.logging
from kubernetes import common
from kubernetes.configuration import Configuration, TestEnvProperties, COMMON_PROPERTIES

EXTRA_ENV = 'extraEnv'
COMMON_EXTRA_ENV = 'commonExtraEnv'
SPLIT_BY = ';'
YAML_INDENTATION = 2


def split_strip(value: str, split_by: str = SPLIT_BY) -> List[str]:
    ret: List[str] = value.split(split_by)
    return list(map(str.strip, ret))


class Component(Enum):
    MMM_BE = 'mmmBe'
    MMM_FE = 'mmmFe'
    DPM = 'dpm'
    DPE = 'dpe'


class HelmValuesFile:
    SIZING_SECTION = 'sizings'
    SUITES_SECTION = 'modules'
    SIZING_PATHS = ['resources', 'extraProperties', EXTRA_ENV, 'javaOpts', 'replicas', 'storage/tmp/sizeLimit']

    def __init__(self, sizing: str, modules_config: str, use_ordered_dict: bool = True):
        self.logger = logging.getLogger(kubernetes.logging.fullname(self))
        self.sizing_folder = common.check_folder(folder=COMMON_PROPERTIES.sizing_folder)
        self.sizing = sizing
        # config parser for sizing
        self.config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        # case-sensitive keys
        self.config.optionxform = str
        self.sizing_ini: Path = common.check_file(Path(self.sizing_folder, f"{self.SIZING_SECTION}.ini"))
        # list of relative paths with sizing definition
        self.config.read(self.sizing_ini)
        self.sizing_rel_ini_files: List[str] = self.config[self.SIZING_SECTION][self.sizing].split(',')
        self.sizing_ini_files: List[Path] = [Path(self.sizing_folder, sf) for sf in self.sizing_rel_ini_files]
        self.config.read(self.sizing_ini_files)
        # both SIZING and component sections will be in config thus remove SIZING_SECTION
        self.sizing_sections: List[str] = self.config.sections()
        self.sizing_sections.remove(self.SIZING_SECTION)
        # config parser for modules
        # self.modules = modules_config
        self.config_modules = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        # case-sensitive keys
        self.config_modules.optionxform = str
        self.modules_ini: Path = common.check_file(Path(self.sizing_folder, f"{self.SUITES_SECTION}.ini"))
        self.config_modules.read(self.modules_ini)
        enabled_modules: List[str] = self.config_modules[self.SUITES_SECTION][modules_config].split(",")
        self.enabled_modules = sorted(list(map(str.strip, enabled_modules)))
        self.disabled_modules: List[str] = self.disabled_mod()
        self.values_doc = self.ordered_list(values=[], ordered_dict=use_ordered_dict)

    def disabled_mod(self):
        """
        check if any of enabled modules does not start with sizing section. If yes DO NOT disable it
        should solve cases when xxx is present in sizing sections and modules section contains xxx/yyy
        example `postgresExporter/serviceMonitor` in modules enables both `postgresExporter` and
        `postgresExporter/serviceMonitor` even if sizing section contains only `[postgresExporter]` section

        Opposite case is by default i.e. when both sizing and modules contain `termSuggestions/api` both
        `termSuggestions` and `api` are enabled
        :return: list of disabled modules
        """
        ret = list(filter(lambda x: not any(y.startswith(x) for y in self.enabled_modules), self.sizing_sections))
        return sorted(ret)

    def load_values_file(self, values_file: Path) -> OrderedDict:
        """ Load existing values yaml file """
        common.check_file(file=values_file)
        with open(values_file, 'r') as f:
            self.logger.info(f"Loading values file from {values_file.resolve()}")
            values_doc = yaml_load(f)
            return values_doc

    def dump_values_file(self, dump_file: Path = None):
        if dump_file:
            self.logger.info(f"Dumping perf values to {dump_file.resolve()}")
            with open(dump_file, 'w') as f:
                yaml_dump(self.values_doc, f, indent=2 * YAML_INDENTATION, block_seq_indent=YAML_INDENTATION)
        else:
            self.logger.info(f"Dump file not specified")

    @staticmethod
    def str_bool(value: Any) -> Any:
        if isinstance(value, str):
            if value.lower() == 'true':
                return True
            elif value.lower() == 'false':
                return False
        return value

    def merge_property(self, key: str, value: Any, dst_dict: dict):
        """
        Creates the field if it does not exist
        Skip None values
        """
        if value is None:
            return
        try:
            value = self.str_bool(value)
            dpath.merge(dst_dict[key], value)
            self.logger.info(f"{key} found, merge {value}")
        except KeyError:
            self.logger.warning(f"{key} not found, create new with value {value}")
            dpath.new(dst_dict, key, value)

    def sizing_value(self, component: str, key: str):
        try:
            ret = self.config[component][key]
            return ret
        except KeyError as e:
            # typer.echo(f"{key} not set for {component}")
            return None

    def java_opts(self, component: str):
        return self.sizing_value(component=component, key='javaOpts')

    def replicas(self, component: str):
        return self.sizing_value(component=component, key='replicas')

    def storage_size_limit(self, component: str):
        return self.sizing_value(component=component, key='storage.tmp.sizeLimit')

    def set_sizing_path(self, rel_path, function: Callable[[str], Any]):
        """
        Set resources for all sections present in sizing config
        """
        for section in self.sizing_sections:
            # extra_envs returns both key and value as one piece of data
            key = f"{section}/{rel_path}" if rel_path is not None else section
            self.logger.debug(f"Setting values for {section}/{rel_path}")
            self.merge_property(key=key, value=function(section), dst_dict=self.values_doc)

    def resources(self, component: str) -> OrderedDict:
        mem_r = ('memory', self.sizing_value(component=component, key='memory.requests'))
        mem_l = ('memory', self.sizing_value(component=component, key='memory.limits'))
        cpu_r = ('cpu', self.sizing_value(component=component, key='cpu.requests'))
        cpu_limits = self.sizing_value(component=component, key='cpu.limits')
        cpu_l = ('cpu', cpu_limits)
        # do not set explicit CPU limits if not specified
        val_l = self.ordered_list([cpu_l, mem_l]) if cpu_limits else self.ordered_list([mem_l])
        val_r = self.ordered_list([cpu_r, mem_r])
        limits = ('limits', val_l)
        requests = ('requests', val_r)
        val_resources = self.ordered_list([requests, limits])
        return val_resources

    def read_extra_properties(self, component: str, split_by: str = SPLIT_BY,
                              key: str = 'extraProperties') -> List[str]:
        """
        read properties from ini file and split them by split_by,
        some properties might contain list delimited by comma so DO NOT use `,`
        for split_by
        """
        value = self.sizing_value(component=component, key=key)
        if value is None:
            return []
        return split_strip(value=value, split_by=split_by)

    def extra_properties(self, component: str) -> Union[LiteralScalarString, None]:
        """ Adds extra properties in format key=value"""
        e_p = self.read_extra_properties(component=component, split_by=SPLIT_BY, key='extraProperties')
        props_as_str = '\n'.join(e_p)
        # empty list/string is false
        return LiteralScalarString(props_as_str) if props_as_str else None

    @staticmethod
    def typed_values(props):
        """
        In yaml numerical and boolean values are by default quoted
        when passed as strings. More complex strings are not quoted
        int and boolean values are not quoted when stored as typed values
        """
        for p in props:
            assert len(p) == 2
            value = p[1]
            try:
                # for float value ValueError is raised when converting to int
                p[1] = int(value)
                continue
            except ValueError:
                try:
                    p[1] = float(value)
                    continue
                except ValueError:
                    pass
            if value.lower() == 'true':
                p[1] = True
                continue
            if value.lower() == 'false':
                p[1] = False

    def extra_properties_map(self, component: str):
        """ Adds extra properties in key: value format"""
        #  list of properties in key=value format
        e_p: List[str] = self.read_extra_properties(component=component, split_by=SPLIT_BY, key='extraProperties')
        return self.properties_map(e_p)

    def properties_map(self, e_p: List[str]):
        e_p_lists = [prop.split('=', maxsplit=1) for prop in e_p]
        # int and boolean values are not wrapped by quotes
        self.typed_values(e_p_lists)
        e_p_tuples: List[tuple] = [tuple(p) for p in e_p_lists]
        return self.ordered_list(e_p_tuples) if e_p_tuples else None

    def get_extra_env(self, component: str):
        """
        for excluded components or when common env vars are not enabled
        returns value which is set in ini file for component
        common env vars are in DEFAULT section of sizing/default/performance.ini
        :param component: component
        :return: value set in ini file or common envs + value set in ini file for component
        """
        exclude_pods_common_envs: List[str] = \
            split_strip(self.config.get(section=component, option='excludePodsCommonExtraEnv'))
        common_envs_enabled: bool = self.config.getboolean(section=component, option='commonExtraEnvEnabled')
        common_envs = self.config.get(section=component, option=COMMON_EXTRA_ENV)
        extra_envs = self.sizing_value(component=component, key=EXTRA_ENV)
        if component in exclude_pods_common_envs or not common_envs_enabled:
            return extra_envs
        else:
            # add SPLIT_BY when concatenated extra env - no SPLIT_BY after last item
            return common_envs + SPLIT_BY + extra_envs if extra_envs is not None else common_envs

    def extra_envs(self, component: str):
        value = self.get_extra_env(component=component)
        if value is None:
            return None
        return self.process_extra_envs(value)

    def process_extra_envs(self, value):
        names_values = [x.split('=') for x in split_strip(value=value)]
        env_vars = []
        for name_value in names_values:
            # one env var is list of 2 tuples
            env_var = [('name', name_value[0]), ('value', name_value[1])]
            env_vars.append(env_var)
        return self.list_of_dicts(env_vars)

    @staticmethod
    def ordered_dict(values: List[list], key: Optional[str] = None) -> OrderedDict:
        z = [OrderedDict(x) for x in values]
        f = OrderedDict([(key, z)])
        return OrderedDict(f)

    @staticmethod
    def list_of_dicts(values: List[list]):
        z = [OrderedDict(x) for x in values]
        return z

    @staticmethod
    def ordered_list(values: List[tuple], ordered_dict: bool = True) -> OrderedDict | Dict:
        z = OrderedList(values)
        return OrderedDict(z) if ordered_dict else {}

    # values taken from env definition i.e. properties not related to sizing

    def postgres_exporter_env(self, properties: TestEnvProperties) -> OrderedDict:
        db_uri = f"{properties.app_db_host_name}:5432/postgres?sslmode=disable"
        uri = [('name', 'DATA_SOURCE_URI'), ('value', db_uri)]
        user = [('name', 'DATA_SOURCE_USER'), ('value', properties.app_db_user)]
        return self.ordered_dict([uri, user], key=EXTRA_ENV)

    def global_db(self, properties: TestEnvProperties) -> OrderedDict:
        db_host = properties.app_db_host_name
        db_port = int(properties.app_db_port)
        h = ('host', db_host)
        p = ('port', db_port)
        un = ('userName', properties.app_db_user)
        pw = ('password', properties.app_db_password)
        return self.ordered_list([h, p, un, pw])

    @staticmethod
    def image_tags(cfg: Configuration) -> dict:
        tag_properties: List[str] = cfg.helm_properties(end_key='tag')
        tags: dict = {}
        for tag_property in tag_properties:
            key = tag_property.split('.')[1:]  # skip common helm
            key = '/'.join(key)  # create path like glob
            value = cfg.get_property(tag_property)
            typer.echo(f"Setting {key}={value}")
            tags.update({key: value})
        return tags

    def set_modules(self, modules: List[str], enabled: bool):
        for module in modules:
            paths: List[str] = module.split("/")
            for i in range(len(paths)):
                path = '/'.join(paths[:i + 1])
                self.set_enabled(module=path, enabled=enabled)

    def set_enabled(self, module: str, enabled: bool):
        entry = ('enabled', enabled)
        value = self.ordered_list([entry])
        self.merge_property(key=module, value=value, dst_dict=self.values_doc)

    def set_all_sizing_paths(self):
        self.set_modules(self.enabled_modules, enabled=True)
        self.set_modules(self.disabled_modules, enabled=False)
        self.set_sizing_path(function=self.resources, rel_path='resources')
        # self.set_sizing_path(function=self.extra_properties, rel_path='extraProperties')
        self.set_sizing_path(function=self.extra_properties_map, rel_path='extraProperties')
        self.set_sizing_path(function=self.extra_envs, rel_path=EXTRA_ENV)
        self.set_sizing_path(function=self.java_opts, rel_path='javaOpts')
        self.set_sizing_path(function=self.replicas, rel_path='replicas')
        self.set_sizing_path(function=self.storage_size_limit, rel_path='storage/tmp/sizeLimit')

    def ini_yaml(self, folder: Path = Path.cwd(), properties: Optional[TestEnvProperties] = None) -> Path:
        """converts ini file to yaml
        :return path to yaml file
        """
        for section in self.sizing_sections:
            # can be dict, str, list[str]
            #  default key = resources
            # noinspection PyTypeChecker
            section_dict = self.add_section(section)
            self.merge_property(dst_dict=self.values_doc, key=section, value=section_dict)
        if properties:
            self.merge_property(dst_dict=self.values_doc, key='postgresExporterMmmDb/db/dbName',
                                value=properties.mmm_be_db_name)
        sizing_yaml = Path(folder, f'{self.sizing}_ini_sizing.yaml')
        sizing_json = Path(folder, f'{self.sizing}_ini_sizing.json')
        msg = f"Writing {sizing_yaml} and {sizing_json}"
        typer.echo(msg)
        with open(sizing_yaml, 'w') as f:
            yaml.dump(self.values_doc, f, width=99999)
        with open(sizing_json, 'w') as f:
            json.dump(self.values_doc, f, indent=2)
        return sizing_yaml.resolve()

    def multiply_resources(self, folder: Path = Path.cwd(), multiply_cpu: float = 1, multiply_mem: float = 1,
                           filter_components: Tuple[str, ...] = ('mmmBe',)):
        """ Multiply resources for filtered components"""
        from kubernetes.helm.manifest_analysis import normalize_metrics
        from kubernetes.helm.manifest_analysis import RESOURCES_KEY
        sizing_df: pd.DataFrame = pd.DataFrame(data=self.values_doc).T
        top_level_resources: pd.Series = sizing_df['resources']
        top_level_resources_not_nan = top_level_resources.dropna()
        n_resources = top_level_resources_not_nan.apply(
            lambda x: {RESOURCES_KEY: normalize_metrics(x, multiply_cpu=multiply_cpu,
                                                        multiply_mem=multiply_mem)})
        n_resources.name = "NORMALIZED_RESOURCES"
        # n_r_df: pd.DataFrame = pd.concat([n_resources, sizing_df], axis=1)
        n_r_df: pd.DataFrame = pd.DataFrame(n_resources)
        filtered_df = n_r_df[n_r_df.index.isin(filter_components)]
        v_f = filtered_df.to_dict()
        sizing_yaml = Path(folder, f'{self.sizing}_{multiply_cpu}x_cpu_{multiply_mem}x_mem.yaml')
        with open(sizing_yaml, 'w') as f:
            yaml.dump(v_f["NORMALIZED_RESOURCES"], f, width=99999)
        return sizing_yaml.resolve()

    def add_section(self, section, is_app_template: bool = False):
        # can be dict, str, list[str]
        #  default key = resources
        # noinspection PyTypeChecker
        value = dict(self.config[section].items())
        path_keys = [k.replace('.', '/') for k in value.keys()]
        path_values = list(value.values())
        section_dict = {}
        for key, value in zip(path_keys, path_values):
            if key == 'extraProperties':
                value = split_strip(value=value, split_by=SPLIT_BY)
                value = dict(self.properties_map(value))
            elif key == 'extraEnv':
                extra_env_values = self.process_extra_envs(value=value)
                value = [dict(x) for x in extra_env_values]
            elif 'cpu' in key or 'memory' in key:
                if is_app_template:
                    key_list: List[str] = key.split('/')
                    key_list.reverse()
                    # reverse and wo resources
                    key = '/'.join(key_list)
                else:
                    key = f'resources/{key}'
            elif 'replicas' in key:
                value = int(value)
            self.merge_property(dst_dict=section_dict, key=key, value=value)
        return section_dict


app = typer.Typer()


@app.command()
def save_yaml(sizing: str = typer.Option('perf_standard'),
              modules: str = typer.Option('basic')):
    # ordered_dict = False means use simple dict for yaml and json
    helm_values_file: HelmValuesFile = HelmValuesFile(sizing=sizing, modules_config=modules,
                                                      use_ordered_dict=False)
    helm_values_file.ini_yaml()
    helm_values_file.multiply_resources(multiply_mem=2, multiply_cpu=3)


BASIC_SIZINGS = ['minimal', 'small', 'standard', 'large', 'xlarge']
PERF_SIZINGS = ['perf_standard', 'perf_minimal']
GENERATED_SIZINGS = ['S01', 'S02', 'S03', 'S04', 'S05', 'S06', 'S07', 'S08', 'S09', 'S10', 'S11']


@app.command()
def app_tmpl(sizings: List[str] = typer.Option(BASIC_SIZINGS),
             modules: str = typer.Option('basic')):
    """Creates and store json to directory based structure used by Portal for application templates"""
    h_v_files: Dict[str, HelmValuesFile] = {sizing: HelmValuesFile(sizing=sizing, modules_config=modules,
                                                                   use_ordered_dict=False)
                                            for sizing in sizings}
    section_sizing_dict = defaultdict(dict)  # solves issue with using missing keys
    # fill in section_sizing_dict
    for sizing, value_file in h_v_files.items():
        for section in value_file.sizing_sections:
            section_sizing_dict[section][sizing] = value_file.add_section(section, is_app_template=True)

    for section in section_sizing_dict.keys():
        app_template_folder = Path('templates', 'services', section, 'sizing')
        os.makedirs(app_template_folder, exist_ok=True)
        last_section = split_strip(section, split_by='/')[-1]
        sizing_yaml = Path(app_template_folder, f'{last_section}.yaml')
        sizing_json = Path(app_template_folder, f'{last_section}.json')
        msg = f'save {section}'
        typer.echo(msg)
        data = {'chartRootKey': section.replace('/', '.'), 'default': {}, 'sizing': section_sizing_dict[section]}
        with open(sizing_yaml, 'w') as f:
            yaml.dump(data=data, stream=f, indent=2, width=5000)
        with open(sizing_json, 'w') as f:
            json.dump(obj=data, fp=f, indent=2)


if __name__ == "__main__":
    app()
