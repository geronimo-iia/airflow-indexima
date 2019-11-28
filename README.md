# airflow-indexima

WORK IN PROGRESS !!

[![Unix Build Status](https://img.shields.io/travis/geronimo-iia/airflow-indexima/master.svg?label=unix)](https://travis-ci.org/geronimo-iia/airflow-indexima)
[![PyPI Version](https://img.shields.io/pypi/v/airflow-indexima.svg)](https://pypi.org/project/airflow-indexima)
[![PyPI License](https://img.shields.io/pypi/l/airflow-indexima.svg)](https://pypi.org/project/airflow-indexima)

Versions following [Semantic Versioning](https://semver.org/)

## Overview

[Indexima](https://indexima.com/) [Airflow](https://airflow.apache.org/) integration based on pyhive.


## Setup

### Requirements

* Python 3.6+

### Installation

Install this library directly into an activated virtual environment:

```text
$ pip install airflow-indexima
```

or add it to your [Poetry](https://poetry.eustace.io/) project:

```text
$ poetry add airflow-indexima
```

## Usage

After installation, the package can imported:

```text
$ python
>>> import airflow_indexima
>>> airflow_indexima.__version__
```

See [Api documentation](https://geronimo-iia.github.io/airflow-indexima/api/)


## Example

### a simple query

```python
from airflow_indexima import IndeximaQueryRunnerOperator

...

with dag:
    ...
    op = IndeximaQueryRunnerOperator(
        task_id = 'my-task-id',
        sql_query= 'DELETE FROM Client WHERE GRPD = 1',
        indexima_conn_id='my-indexima-connection'
    )
    ...
```


### a load into indexima

```python
from airflow_indexima import IndeximaLoadDataOperator

...

with dag:
    ...
    op = IndeximaLoadDataOperator(
        task_id = 'my-task-id',
        indexima_conn_id='my-indexima-connection',
        target_table='Client',
        source_select_query='select * from dsi.client',
        truncate=True,
        load_path_uri='jdbc:redshift://my-private-instance.com:5439/db_client?ssl=true&user=airflow-user&password=XXXXXXXX'
    )
    ...

```

## License

[The MIT License (MIT)](https://geronimo-iia.github.io/airflow-indexima/license)


## Contributing

See [Contributing](https://geronimo-iia.github.io/airflow-indexima/contributing)

