#!/usr/bin/env python
# -*- coding: utf-8 -*-

import shutil

from md_autogen import MarkdownAPIGenerator
from md_autogen import to_md_file

from airflow_indexima.hooks import indexima as hooks

MKDOC_BUILD_DIR = ".cache/mkdocs"
GIT_URL = "https://github.com/geronimo-iia/airflow-indexima/tree/master"


def generate_api_docs():
    modules = [
        hooks,
    ]

    md_gen = MarkdownAPIGenerator("vis", GIT_URL)
    for m in modules:
        md_string = md_gen.module2md(m)
        to_md_file(md_string, m.__name__, "sources")


def update_md():
    shutil.copyfile('../README.md', f'{MKDOC_BUILD_DIR}/index.md')
    shutil.copyfile('../CHANGELOG.md', f'{MKDOC_BUILD_DIR}/changelog.md')
    shutil.copyfile('../CONTRIBUTING.md', f'{MKDOC_BUILD_DIR}/contributing.md')
    shutil.copyfile('../CODE_OF_CONDUCT.md', f'{MKDOC_BUILD_DIR}/code_of_conduct.md')


def cleanup():
    shutil.rmtree(MKDOC_BUILD_DIR, ignore_errors=True)


if __name__ == "__main__":
    cleanup()
    update_md()
    generate_api_docs()
