import configparser
import json
import os
from pathlib import Path
from typing import List, Union

from loguru import logger

from cmdline import common
from cmdline.constants import CPT_HOME, API_TEST_MODULE


def url_hostname(fe_url: str, api_url: str) -> str:
    """extract hostname from URL"""
    from urllib.parse import urlparse
    parsed_fe_uri = urlparse(url=fe_url)
    parsed_api_uri = urlparse(url=api_url)
    fe_hostname = '{uri.netloc}'.format(uri=parsed_fe_uri)
    api_hostname = '{uri.netloc}'.format(uri=parsed_api_uri)
    if fe_hostname != api_hostname:
        msg = f'API and FE hostnames are different : {api_hostname} != {fe_hostname}'
        logger.warning(msg)
    ret: dict = {'fe_hostname': fe_hostname, 'api_hostname': api_hostname}
    return json.dumps(ret)


class Configuration:
    CONFIG_SECTION = 'Main'
    TEST_ENV_KEY = "test_env"
    # CPT_HOME_KEY = 'CPT_HOME'
    SNOWFLAKE_PASSWORD_KEY = 'SNOWFLAKE_PASSWORD'
    SNOWFLAKE_USER_KEY = 'SNOWFLAKE_USER'
    RESOURCES_FOLDER = 'resources'
    # CPT_HOME is used to access properties files in Java API tests
    # If the new structure puts pycpt and jcpt to the same level path of Java CPT
    # must be added to resources
    # CPT_HOME = os.getenv(CPT_HOME_KEY)
    SNOWFLAKE_USER = os.getenv(SNOWFLAKE_USER_KEY)
    SNOWFLAKE_PASSWORD = os.getenv(SNOWFLAKE_PASSWORD_KEY)

    # to be changed when moving resources to pycpt folder

    def __init__(self, test_env: str):
        logger.info(f"Reading configuration for test env = {test_env}")
        self.config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        # case-sensitive keys
        self.config.optionxform = str
        self.test_env = test_env
        self.meta_properties_file = common.check_file(Path(API_TEST_MODULE, self.RESOURCES_FOLDER,
                                                           self.test_env + '.properties'))
        # env.resources=resources/common;resources/paas/common;resources/paas/perf
        self.config.read(self.meta_properties_file)
        self.rel_properties_folders: List[str] = self.config[self.CONFIG_SECTION]["env.resources"].split(';')
        # relative path of property file contains `resources` folder
        self.properties_files: List[Path] = [Path(API_TEST_MODULE, f"{prop_folder}/test.properties")
                                             for prop_folder in self.rel_properties_folders]
        self.config.read(self.properties_files)

    def get_property(self, key: str) -> Union[str, None]:
        """
        Load the env var with key created from property key by replacing '.' by '_' and
        converting to upper case
        :param key:
        :return: value of the env var if exists or property
        """
        try:
            env_key = key.replace('.', '_').upper()
            env_value = os.getenv(env_key)
            ret = env_value if env_value is not None else self.config[self.CONFIG_SECTION][key]
            logger.debug(f"{key} = {ret}")
            return ret
        except KeyError:
            logger.warning(f"{key} not found")
            return None

    def get_cpt_path_property(self, key: str) -> Path:
        """ Absolute path from relative to CPT_HOME path"""
        ret = self.get_property(key=key)
        if ret is not None:
            ret = Path(CPT_HOME, ret).resolve()
            return ret
        else:
            raise KeyError(f"{key} not found")

    def helm_properties(self, end_key: str = None) -> List[str]:
        """Filters properties started with `helm` and optionally ended by end_key"""
        keys = self.config[self.CONFIG_SECTION].keys()
        all_helm_keys = [x for x in keys if str(x).startswith('helm')]
        if not end_key:
            return all_helm_keys
        else:
            return [x for x in all_helm_keys if str(x).split('.')[-1] == end_key]


class TestEnvProperties:
    DEFAULT_TEST_ENV = "paas_common"

    def __init__(self, test_env: str = DEFAULT_TEST_ENV):
        """
        Sets the properties for the test env. Not all keys are present in DEFAULT_TEST_ENV
        because they differ in different test env
        """
        logger.info(f"Reading properties for test env = {test_env}")
        self.test_env = test_env
        self.cfg = Configuration(test_env=test_env)
        self.cpt_artefacts_dir: Path = self.cfg.get_cpt_path_property("cmdline.artefacts.rel.dir")
        self.helm_charts_repo: Path = self.cfg.get_cpt_path_property("helm.charts.git.dir")
        self.cpt_sizing_repo: Path = self.cfg.get_cpt_path_property("helm.cmdline.git.dir")
        # relative to CPT_HOME folder where all helm build install and report files are stored
        # replaces helm.performance.values.install.dir property
        self.helm_perf_values_dir: Path = Path(self.cpt_artefacts_dir, "helm_builds")
        self.kustomize_git_dir: Path = self.cfg.get_cpt_path_property('kustomize.git.dir')
        self.lr_vugen_scripts_git_dir: Path = self.cfg.get_cpt_path_property('lr.vugen.scripts.git.dir')
        self.lr_vugen_scripts: List[str] = self.property_to_list(self.cfg.get_property('lr.vugen.scripts'))
        self.lr_vugen_docflow_scripts: List[str] = \
            self.property_to_list(self.cfg.get_property('lr.vugen.docflow.scripts'))
        self.lr_controller_script: str = self.cfg.get_property('lr.controller.script')
        self.lr_controller_scripts_git_dir: Path = self.cfg.get_cpt_path_property('lr.controller.scripts.git.dir')
        self.generated_data_dir: Path = self.cfg.get_cpt_path_property("generated.data.rel.dir")
        self.minio_lookup_dir: Path = self.cfg.get_cpt_path_property("minio.lookup.rel.dir")
        self.backup_data_dir: Path = self.cfg.get_cpt_path_property("backup.data.rel.dir")
        self.app_host_name = self.cfg.get_property("app.host")
        self.app_namespace = self.cfg.get_property("app.namespace")
        self.app_db_password = self.cfg.get_property('app.db.password')
        self.dpm_db_password = self.cfg.get_property('dpm.db.password')
        self.app_db_user = self.cfg.get_property('app.db.user')
        self.dpm_db_user = self.cfg.get_property('dpm.db.user')
        self.app_db_role = self.cfg.get_property('app.db.role')
        self.app_db_host_name = self.cfg.get_property('db.host')
        self.app_db_port = self.cfg.get_property('db.port')
        self.kustomize_build_dir = self.cfg.get_property('kustomize.build.dir')
        self.kustomize_interpolation_file = self.cfg.get_property('kustomize.interpolated.file')
        self.testng_suite_rel_dir = self.cfg.get_property('test.suite.rel.dir')
        self.test_results_rel_dir = self.cfg.get_property('test.results.rel.dir')
        # FE_APP_URL
        self.fe_app_url = self.cfg.get_property('fe.app.url')
        self.mmm_be_db_name = self.cfg.get_property("app.db.name")
        self.dpm_db_name = self.cfg.get_property("dpm.db.name")
        # Used in com.ataccama.one.performance.service.ApiService#getBaseUri
        # via Constants.getTestEnvironmentProperty("app.graphql.url");
        self.mmm_be_graphql_url = self.cfg.get_property('app.graphql.url')
        # used in com.ataccama.one.performance.service.ApiService#API_USERNAME
        # Constants.getTestEnvironmentProperty("api.test.username");
        self.mmm_be_graphql_username = self.cfg.get_property('api.test.username')
        # API_PASSWORD = Constants.getTestEnvironmentProperty("api.test.password");
        self.mmm_be_graphql_password = self.cfg.get_property('api.test.password')
        self.mmm_be_actuator_info_url = self.cfg.get_property('mmm-be.actuator.info.url')
        self.dpm_actuator_info_url = self.cfg.get_property('dpm.actuator.info.url')
        self.dpe_actuator_info_url = self.cfg.get_property('dpe.actuator.info.url')
        self.minio_alias_name = self.cfg.get_property("minio.alias.name")
        self.minio_url = self.cfg.get_property("minio.url")
        self.minio_access_key = self.cfg.get_property("minio.access.key")
        self.minio_secret_key = self.cfg.get_property("minio.secret.key")
        self.kube_context = self.cfg.get_property("kube.context")
        self.document_connections = self.cfg.get_property("document.connections")
        self.documentation_cfg = self.cfg.get_property("documentation.configuration")
        # calculated properties/constants - get rid of string literals in code
        self.charts_path = Path(self.helm_charts_repo, 'ataccama-one/charts/ataccama-one').resolve()
        # values updated by gitlab admin bot
        self.ondemand_testing_values = Path(self.helm_charts_repo, 'values_templates', 'ondemand-testing-values.yaml')
        self.full_portal_values = Path(self.helm_charts_repo, 'values_templates',
                                       'values-full-one-portal-no-services.yaml')
        self.patch_folder = Path(self.cpt_sizing_repo, 'helm')
        self.perf_values_template: Path = Path(self.cpt_sizing_repo, 'helm', 'perf-values-template.yaml')
        self.sizing_folder = Path(self.cpt_sizing_repo, 'sizing')
        self.runtime_properties = {}

    def test_env_extended(self) -> dict:
        ret = {Configuration.TEST_ENV_KEY: self.test_env,
               "app_graphql_url": self.mmm_be_graphql_url,
               "fe_app_url": self.fe_app_url,
               "hostnames": url_hostname(fe_url=self.fe_app_url,
                                         api_url=self.mmm_be_graphql_url)}
        return ret

    def add_runtime_property(self, key: str, value):
        self.runtime_properties[key] = value

    def get_runtime_property(self, prop: str):
        try:
            ret = self.runtime_properties[prop]
            return ret
        except KeyError:
            return None

    @staticmethod
    def property_to_list(raw_property: str, split_by: str = ',') -> List[str]:
        if raw_property is not None:
            return raw_property.split(split_by)
        else:
            return []


# single instance for common properties
COMMON_PROPERTIES = TestEnvProperties()
