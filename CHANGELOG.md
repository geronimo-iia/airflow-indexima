# Change Log

## 2.2.2

- upgrade project template

## 2.2.1 (2019-12-17)

- fix api link on readme
- fix readme info
- add optional pause between truncate/load/commit operation

## 2.2.0 (2019-12-12)

- add dry_run mode on operator
- refactor uri module (simplier code)
- add jdbc uri generator for postgresql
- support full syntax of load path query
- add a bunch of test unit (not too soon...) and reactivate coverage
- integrate hive transport factory in order to manage socket configuration
- add support for authentication mode: ldap, custom, kerberos and none
- add more documentation
- use thrift 0.13.0
- use a new connection on rollback
- log original error before rollback
- use information from https://www.ericlin.me/2015/07/how-to-configue-session-timeout-in-hive/
- add 'hive_configuration' member to IndeximaHook
- change default time out to None
- use a single cursor instance per hook process
- set hive connection serialization encoding 'UTF-8'
- timeout can be specified with an int or a timedelta
- use execution_timeout member on operator to set timeout if this one is not specified

## 2.1.0 (2019-12-04)

- manage error return from indexima
- define IndeximaAirflowPlugin

## 2.0.6 (2019-12-03)

- fix usage of connection.extra parameter

## 2.0.5 (2019-12-03)

- fix indexima hook:
  - (username/login)
  - field access (auth)
  - decorator applied before usage
- fix IndeximaLoadDataOperator field access
- fix redshit uri base

## 2.0.4 (2019-12-03)

- fix connection retrieval in get_redshift_load_path_uri
- fix redshit uri port
  
## 2.0.3 (2019-12-03)

- align dependencies constraint on thrift to pyhive and thrift-sasl

## 2.0.2 (2019-12-03)

- unlock fixed python 3.6.4 to ^3.6

## 2.0.1 (2019-12-03)

- fix default truncate query

## 2.0.0 (2019-12-03)

- escape quote in select query of RedshiftIndexima Operator
- initiate airflow contrib package
- complete docstyle
- introduce uri utilities
- expose ConnectionDecorator
- add more example

## 1.0.1 (2019-11-28)

- add example
- remove work in progress

## 1.0.0 (2019-11-27)

- initial project structure based on [geronimo-iia/template-python](https://github.com/geronimo-iia/template-python)
- add Hook implementation
- add Simple Operator
- add pyhive, ...
- configure documentation
- add a way to customize credentials retreival (with a prepare connection function handler)

