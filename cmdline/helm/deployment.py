import cmdline.helm.kubectl_commands
from cmdline.configuration import TestEnvProperties
from cmdline.psql_utils import PsqlUtils


class TestInfo:
    def __init__(self, test_env: str):
        self.properties: TestEnvProperties = TestEnvProperties(test_env=test_env)
        self.psql_utils = PsqlUtils(test_env=test_env)
        self.mmm_db_metrics = self.psql_utils.mmm_metrics()
        self.deployment_info = cmdline.helm.kubectl_commands.deployment_info(kube_context=self.properties.kube_context,
                                                                             test_env=test_env)
