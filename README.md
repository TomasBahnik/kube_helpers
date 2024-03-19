## Resource Analysis and Sizing

### Install and test

* install [Miniconda](https://docs.conda.io/en/latest/miniconda.html)

```shell
export ENV_NAME=k8s_analysis
conda env remove --name $ENV_NAME
conda create --name $ENV_NAME -c conda-forge python=3.11
conda activate $ENV_NAME
# poetry installed by pip inside the conda env - if not installed yet
pip install poetry
poetry install
pytest -svv
```

### CLI Commands


### Report Examples
