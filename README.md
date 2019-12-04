# airflow-indexima


[![Unix Build Status](https://img.shields.io/travis/geronimo-iia/airflow-indexima/master.svg?label=unix)](https://travis-ci.org/geronimo-iia/airflow-indexima)
[![PyPI Version](https://img.shields.io/pypi/v/airflow-indexima.svg)](https://pypi.org/project/airflow-indexima)
[![PyPI License](https://img.shields.io/pypi/l/airflow-indexima.svg)](https://pypi.org/project/airflow-indexima)

Versions following [Semantic Versioning](https://semver.org/)

## Overview

[Indexima](https://indexima.com/) [Airflow](https://airflow.apache.org/) integration based on pyhive.

This project is used in our prod environment with success.
As it a young project, take care of change, any help is welcome :)


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

or you could use it as an [Airflow plugin](https://airflow.apache.org/docs/stable/plugins.html)

## Usage

After installation, the package can imported:

```text
$ python
>>> import airflow_indexima
>>> airflow_indexima.__version__
```

See [Api documentation](https://geronimo-iia.github.io/airflow-indexima/api/)


### a simple query

```python
from airflow_indexima.operators import IndeximaQueryRunnerOperator

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
from airflow_indexima.operators.indexima import IndeximaLoadDataOperator

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

### customize credential access

If you use another backend to store your password (like AWS SSM), you could define a decorator
and use it as a function in your dag.

```python
from airflow.models import Connection
from airflow import DAG

from airdlow_indexima.uri import define_load_path_factory, get_redshift_load_path_uri


def my_decorator(conn:Connection) -> Connection:
    conn.password = get_ssm_parameter(param_name=f'{conn.conn_id}.{con.login}')
    return conn


dag = DAG(
    dag_id='my_dag',
    user_defined_macros={
        # we define a macro get_load_path_uri
        'get_load_path_uri': define_load_path_factory(
            conn_id='my-redshift-connection',
            decorator=my_decorator,
            factory=get_redshift_load_path_uri)
        },
    ...
)

with dag:
    ...
    op = IndeximaLoadDataOperator(
        task_id = 'my-task-id',
        indexima_conn_id='my-indexima-connection',
        target_table='Client',
        source_select_query='select * from dsi.client',
        truncate=True,
        load_path_uri='{{ get_load_path_uri() }}'
    )
    ...


```

a Connection decorator must follow this type: ```ConnectionDecorator = Callable[[Connection], Connection]```

```define_load_path_factory``` is a function which take:

- a connnection identifier
- a decorator ```ConnectionDecorator```
- an uri factory ```UriGeneratorFactory = Callable[[str, Optional[ConnectionDecorator]], str]```

and return a function with no argument which can be called as a macro in dag's operator.


## License

[The MIT License (MIT)](https://geronimo-iia.github.io/airflow-indexima/license)


## Contributing

See [Contributing](https://geronimo-iia.github.io/airflow-indexima/contributing)

