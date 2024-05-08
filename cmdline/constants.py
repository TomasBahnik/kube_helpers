import os
import re
from pathlib import Path

MIBS: int = 1024 ** 2
GIBS: int = 1024 ** 3

CPT_HOME_KEY = 'CPT_HOME'
CPT_HOME = os.getenv(CPT_HOME_KEY)
PYCPT_HOME = Path(CPT_HOME, 'bin', 'pycpt')
API_TEST_MODULE = Path(CPT_HOME, 'modules', 'api-tests').resolve()

LINKED_TABLES_PERCENTAGE = '1-n relations between tables (total percentage of linked tables)'
MAX_COLUMNS_PER_TABLE = 'Max columns per table'
HIGH_AVG_COLUMNS_PER_TABLE = 'High average columns per table'
AVG_COLUMNS_PER_TABLE = 'Average columns per table'

common_data_volume = {AVG_COLUMNS_PER_TABLE: 20,
                      HIGH_AVG_COLUMNS_PER_TABLE: 300,
                      MAX_COLUMNS_PER_TABLE: 5000,
                      LINKED_TABLES_PERCENTAGE: 25,
                      'Average # of terms assigned on table level': 4,
                      'Max # of terms assigned on table level': 11,
                      'Average # of terms assigned on column level': 4,
                      'High average # of terms assigned on column level': 9,
                      'Max # of terms assigned on column level': 25,
                      'Average historical versions of entity': 5,
                      'Max historical version of entity': 20}
# 300Mi/Gi/m
RESOURCE_RE = re.compile(r"(\d+\.?\d*)([a-zA-Z]*)$")
units_conversion = {"Mi": MIBS, "Gi": GIBS, "G": GIBS, "m": 0.001}
MMM_BUILD_INFO_GQL = {
    "query": "query getBuildInfo {\n  _buildInfo {\n    branch\n    buildHost\n    buildUserName\n    buildVersion\n  "
             "  commitId\n    commitIdAbbrev\n    commitTime\n    commitUserName\n    totalCommitCount\n  }\n}\n",
    "operationName": "getBuildInfo"}
# use for all pd.Timestamps - bad conversion to Snowflake TIMESTAPMPNTZ
GMT_TZ = 'GMT'
