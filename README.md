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

See [Api documentation](https://geronimo-iia.github.io/airflow-indexima/)


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
### get load path uri from Connection

In order to get jdbc uri from an Airflow Connection, you could use:

- ```get_redshift_load_path_uri```
- ```get_postgresql_load_path_uri```

from module ```airflow_indexima.uri```

Both method have this profile: ```Callable[[str, Optional[ConnectionDecorator]], str]```


Example:
```
    get_postgresql_load_path_uri(connection_id='my_conn')
    >> 'jdbc:postgresql://my-db:5432/db_client?ssl=true&user=airflow-user&password=XXXXXXXX'
```

## Indexima Connection


### Authentication

PyHive supported authentication mode:

- 'NONE': needs a username without password
- 'CUSTOM': needs a username and password (default mode)
- 'LDAP': needs a username and password
- 'KERBEROS': need a kerberos service name
- 'NOSASL': corresponds to hive.server2.authentication=NOSASL in hive-site.xml


### Configuration

You could set those parameters:

- host (str): The host to connect to.
- port (int): The (TCP) port to connect to.
- timeout_seconds ([int]): define the socket timeout in second (default None)
- socket_keepalive ([bool]): enable TCP keepalive, default false.
- auth (str): authentication mode
- username ([str]): username to login
- password ([str]): password to login
- kerberos_service_name ([str]): kerberos service name

`host`, `port`, `username` and `password` came from airflow Connection configuration.

`timeout_seconds`, `socket_keepalive`, `auth` and `kerberos_service_name` parameters can came from:

1. attribut on Hook/Operator class
2. Airflow Connection in ```extra``` parameter, like this:
   ```
   '{"auth": "CUSTOM", "timeout_seconds": 90, "socket_keepalive": true}'
   ```

Setted attribut override airflow connection configuration.

You could add a decorator function in order to post process Connection before usage.
This decorator will be executed after connection configuration (see next section).

### customize Connection credential access

If you use another backend to store your password (like AWS SSM), you could define a decorator
and use it as a function in your dag.

```python
from airflow.models import Connection
from airflow import DAG

from airdlow_indexima.uri import define_load_path_factory, get_redshift_load_path_uri


def my_decorator(conn:Connection) -> Connection:
    # conn instance will be not shared, and use only on connection request
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

### Optional connection parameters

On each operator you could set this member:

- auth (Optional[str]): authentication mode (default: {'CUSTOM'})
- kerberos_service_name (Optional[str]): optional kerberos service name
- timeout_seconds (Optional[Union[int, datetime.timedelta]]): define the socket timeout in second
                (could be an int or a timedelta)
- socket_keepalive (Optional[bool]): enable TCP keepalive.

Note:

- if execution_timeout is set, it will be used as default value for timeout_seconds.

## Production Feedback

In production, you could have few strange behaviour like those that we have meet.

### "TSocket read 0 bytes" 

You could fine this issue https://github.com/dropbox/PyHive/issues/240 on long load query running.

Try this in sequence:

1. check your operator configuration, and set ```timeout_seconds``` member to 3600 second for example.
   You could have a different behaviour when running a dag with/without airflow context in docker container.
2. if your facing a broken pipe, after 300s, and you have an AWS NLB V2 :
   Read again [network-load-balancers](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html), and focus on this:
   > Elastic Load Balancing sets the idle timeout value for TCP flows to 350 seconds. You cannot modify this value. For TCP listeners, clients or targets can use TCP keepalive packets to reset the idle timeout. TCP keepalive packets are not supported for TLS listeners.

   We have tried for you the "socket_keep_alive", and it did not work at all.
   Our solution was to remove our NLB and use a simple dns A field on indexima master.


### "utf-8" or could not read byte ...

Be very welcome to add ```{ "serialization.encoding": "utf-8"}``` in hive_configuration member of IndeximaHook.

This setting is set in IndeximaHook.__init__, may you override it ?


## Playing Airflow without Airflow Server

When I was trying many little things and deals with hive stuff, i wrote a single script that help me a lot.

Feel free to use it (or not) to set your dag by yourself:

```python
import os
import datetime
from airflow.hooks.base_hook import BaseHook
from airflow import DAG
from airflow_indexima.operators.indexima import IndeximaLoadDataOperator

# here we create our Airflow Connection
os.environ['AIRFLOW_CONN_INDEXIMA_ID'] = 'hive://my-user:my-password@my-server:10000/default'
conn = BaseHook.get_connection('indexima_id')

dag = DAG(
    dag_id='my_dag',
    default_args={
        'start_date': datetime.datetime(year=2019, month=12, day=1),
        'depends_on_past': False,
        'email_on_failure': False,
        'email': [],
    },
)

with dag:

    load_operator = IndeximaLoadDataOperator(
        task_id='my_task',
        indexima_conn_id='indexima_id',
        target_table='my_table',
        source_select_query=(
            "select * from source_table where "
            "creation_date_tms between '2019-11-30T00:00:00+00:00' and '2019-11-30T12:59:59.000999+00:00'"
        ),
        truncate=True,
        truncate_sql=(
            "DELETE FROM my_table WHERE "
            "creation_date_tms between '2019-11-30T00:00:00+00:00' and '2019-11-30T12:59:59.000999+00:00'"
        ),
        load_path_uri='jdbc:postgresql://myserver:5439/db_common?user=etl_user&password=a_strong_password&ssl=true',
        retries=2,
        execution_timeout=datetime.timedelta(hours=3),
        sla=datetime.timedelta(hours=1, minutes=30),
    )

    # here we run the dag
    load_operator.execute(context={})

del os.environ['AIRFLOW_CONN_INDEXIMA_ID']


```


## License

[The MIT License (MIT)](https://geronimo-iia.github.io/airflow-indexima/license)


## Contributing

See [Contributing](https://geronimo-iia.github.io/airflow-indexima/contributing)

### Thanks

Thanks to [@bartosz25](https://github.com/bartosz25) for his help with hive connection details... 
