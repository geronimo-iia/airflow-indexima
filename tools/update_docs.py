#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import shutil

from md_autogen import MarkdownAPIGenerator
from md_autogen import to_md_file

ROOT = os.path.abspath(os.path.join(os.path.dirname(__name__)))
sys.path.append(ROOT)

# from airflow_indexima.hooks import indexima as hooks  # noqa: F401
MKDOC_BUILD_DIR = ".cache/mkdocs"
GIT_URL = "https://github.com/geronimo-iia/airflow-indexima/tree/master"

import airflow_indexima.hooks.indexima as hooks
import airflow_indexima.operators.indexima as operators
import airflow_indexima.connection as connection
import airflow_indexima.hive_transport as hive_transport
import airflow_indexima.uri as uri
import airflow_indexima.uri.factory as factory
import airflow_indexima.uri.jdbc as jdbc

def generate_api_docs():
    modules = [hooks, operators, connection, hive_transport, uri, factory, jdbc]

    md_gen = MarkdownAPIGenerator(src_root="airflow_indexima", github_link=GIT_URL)
    for m in modules:
        md_string = md_gen.module2md(module=m)
        #print(md_string)
        to_md_file(md_string, m.__name__, MKDOC_BUILD_DIR)


def update_md():
    shutil.copyfile(os.path.join(ROOT, 'README.md'), f'{MKDOC_BUILD_DIR}/index.md')
    for target in ['CHANGELOG.md', 'CONTRIBUTING.md', 'CODE_OF_CONDUCT.md', 'LICENSE.md']:
        shutil.copyfile(os.path.join(ROOT, target), os.path.join(MKDOC_BUILD_DIR, target.lower()))


def cleanup():
    shutil.rmtree(MKDOC_BUILD_DIR, ignore_errors=True)
    os.makedirs(MKDOC_BUILD_DIR, exist_ok=True)


if __name__ == "__main__":
    cleanup()
    update_md()
    generate_api_docs()
