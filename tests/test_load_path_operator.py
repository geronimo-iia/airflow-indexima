import pytest


def test_data_operator_operator_fields_has_macro_support():
    from airflow_indexima.operators.indexima import IndeximaLoadDataOperator

    assert '_target_table' in IndeximaLoadDataOperator.template_fields
    assert '_load_path_uri' in IndeximaLoadDataOperator.template_fields
    assert '_source_select_query' in IndeximaLoadDataOperator.template_fields
    assert '_truncate_sql' in IndeximaLoadDataOperator.template_fields
    assert '_format_query' in IndeximaLoadDataOperator.template_fields
    assert '_prefix_query' in IndeximaLoadDataOperator.template_fields
    assert '_skip_lines' in IndeximaLoadDataOperator.template_fields
    assert '_no_check' in IndeximaLoadDataOperator.template_fields
    assert '_limit' in IndeximaLoadDataOperator.template_fields
    assert '_locale' in IndeximaLoadDataOperator.template_fields


def test_load_data_operator_target_table_mandatory():
    from airflow_indexima.operators.indexima import IndeximaLoadDataOperator

    with pytest.raises(TypeError):
        IndeximaLoadDataOperator(
            task_id="my_task", indexima_conn_id='fake_connection_id', load_path_uri="fake uri"
        )


def test_load_data_operator_load_path_uri_mandatory():
    from airflow_indexima.operators.indexima import IndeximaLoadDataOperator

    with pytest.raises(TypeError):
        IndeximaLoadDataOperator(
            task_id="my_task", indexima_conn_id='fake_connection_id', target_table="fake_table"
        )


def test_load_data_operator_generate_query_basic():
    from airflow_indexima.operators.indexima import IndeximaLoadDataOperator

    assert IndeximaLoadDataOperator(
        task_id="my_task",
        indexima_conn_id='fake_connection_id',
        target_table="fake_table",
        load_path_uri="fake:uri//dummy",
    ).generate_load_data_query() == ("LOAD DATA INPATH 'fake:uri//dummy' \nINTO TABLE fake_table;")


def test_load_data_operator_generate_query_with_format():
    from airflow_indexima.operators.indexima import IndeximaLoadDataOperator

    assert IndeximaLoadDataOperator(
        task_id="my_task",
        indexima_conn_id='fake_connection_id',
        target_table="fake_table",
        load_path_uri="fake:uri//dummy",
        format_query="CSV SEPARATOR 'separator'",
    ).generate_load_data_query() == (
        "LOAD DATA INPATH 'fake:uri//dummy' \n" "INTO TABLE fake_table \n" "FORMAT CSV SEPARATOR 'separator';"
    )


def test_load_data_operator_generate_query_with_format_and_prefix():
    from airflow_indexima.operators.indexima import IndeximaLoadDataOperator

    assert IndeximaLoadDataOperator(
        task_id="my_task",
        indexima_conn_id='fake_connection_id',
        target_table="fake_table",
        load_path_uri="fake:uri//dummy",
        format_query="CSV SEPARATOR 'separator'",
        prefix_query="2017\\t2\\t1\\t",
    ).generate_load_data_query() == (
        "LOAD DATA INPATH 'fake:uri//dummy' \n"
        "INTO TABLE fake_table \n"
        "FORMAT CSV SEPARATOR 'separator' \n"
        "PREFIX '2017\\t2\\t1\\t';"
    )


def test_load_data_operator_generate_query_with_query():
    from airflow_indexima.operators.indexima import IndeximaLoadDataOperator

    assert IndeximaLoadDataOperator(
        task_id="my_task",
        indexima_conn_id='fake_connection_id',
        target_table="fake_table",
        load_path_uri="fake:uri//dummy",
        source_select_query="select * from tutu where custom = 'aa'",
    ).generate_load_data_query() == (
        "LOAD DATA INPATH 'fake:uri//dummy' \n"
        "INTO TABLE fake_table \n"
        "QUERY 'select * from tutu where custom = \\'aa\\'';"
    )


def test_load_data_operator_generate_query_with_format_and_prefix_and_query():
    from airflow_indexima.operators.indexima import IndeximaLoadDataOperator

    assert IndeximaLoadDataOperator(
        task_id="my_task",
        indexima_conn_id='fake_connection_id',
        target_table="fake_table",
        load_path_uri="fake:uri//dummy",
        format_query="CSV SEPARATOR 'separator'",
        prefix_query="2017\\t2\\t1\\t",
        source_select_query="select * from tutu where custom = 'aa'",
    ).generate_load_data_query() == (
        "LOAD DATA INPATH 'fake:uri//dummy' \n"
        "INTO TABLE fake_table \n"
        "FORMAT CSV SEPARATOR 'separator' \n"
        "PREFIX '2017\\t2\\t1\\t' \n"
        "QUERY 'select * from tutu where custom = \\'aa\\'';"
    )


def test_load_data_operator_generate_query_with_format_and_prefix_and_query_and_skip():
    from airflow_indexima.operators.indexima import IndeximaLoadDataOperator

    assert IndeximaLoadDataOperator(
        task_id="my_task",
        indexima_conn_id='fake_connection_id',
        target_table="fake_table",
        load_path_uri="fake:uri//dummy",
        format_query="CSV SEPARATOR 'separator'",
        prefix_query="2017\\t2\\t1\\t",
        source_select_query="select * from tutu where custom = 'aa'",
        skip_lines=2,
    ).generate_load_data_query() == (
        "LOAD DATA INPATH 'fake:uri//dummy' \n"
        "INTO TABLE fake_table \n"
        "FORMAT CSV SEPARATOR 'separator' \n"
        "PREFIX '2017\\t2\\t1\\t' \n"
        "QUERY 'select * from tutu where custom = \\'aa\\'' \n"
        "SKIP 2;"
    )


def test_load_data_operator_generate_query_with_format_and_prefix_and_query_and_skip_no_check():
    from airflow_indexima.operators.indexima import IndeximaLoadDataOperator

    assert IndeximaLoadDataOperator(
        task_id="my_task",
        indexima_conn_id='fake_connection_id',
        target_table="fake_table",
        load_path_uri="fake:uri//dummy",
        format_query="CSV SEPARATOR 'separator'",
        prefix_query="2017\\t2\\t1\\t",
        source_select_query="select * from tutu where custom = 'aa'",
        skip_lines=2,
        no_check=True,
    ).generate_load_data_query() == (
        "LOAD DATA INPATH 'fake:uri//dummy' \n"
        "INTO TABLE fake_table \n"
        "FORMAT CSV SEPARATOR 'separator' \n"
        "PREFIX '2017\\t2\\t1\\t' \n"
        "QUERY 'select * from tutu where custom = \\'aa\\'' \n"
        "SKIP 2 \n"
        "NOCHECK;"
    )


def test_load_data_operator_generate_query_with_format_and_prefix_and_query_and_skip_no_check_limit():
    from airflow_indexima.operators.indexima import IndeximaLoadDataOperator

    assert IndeximaLoadDataOperator(
        task_id="my_task",
        indexima_conn_id='fake_connection_id',
        target_table="fake_table",
        load_path_uri="fake:uri//dummy",
        format_query="CSV SEPARATOR 'separator'",
        prefix_query="2017\\t2\\t1\\t",
        source_select_query="select * from tutu where custom = 'aa'",
        skip_lines=2,
        no_check=True,
        limit=1000,
    ).generate_load_data_query() == (
        "LOAD DATA INPATH 'fake:uri//dummy' \n"
        "INTO TABLE fake_table \n"
        "FORMAT CSV SEPARATOR 'separator' \n"
        "PREFIX '2017\\t2\\t1\\t' \n"
        "QUERY 'select * from tutu where custom = \\'aa\\'' \n"
        "SKIP 2 \n"
        "NOCHECK \n"
        "LIMIT 1000;"
    )


def test_load_data_operator_generate_query_with_format_and_prefix_and_query_and_skip_no_check_limit_locale():
    from airflow_indexima.operators.indexima import IndeximaLoadDataOperator

    assert IndeximaLoadDataOperator(
        task_id="my_task",
        indexima_conn_id='fake_connection_id',
        target_table="fake_table",
        load_path_uri="fake:uri//dummy",
        format_query="CSV SEPARATOR 'separator'",
        prefix_query="2017\\t2\\t1\\t",
        source_select_query="select * from tutu where custom = 'aa'",
        skip_lines=2,
        no_check=True,
        limit=1000,
        locale="fr",
    ).generate_load_data_query() == (
        "LOAD DATA INPATH 'fake:uri//dummy' \n"
        "INTO TABLE fake_table \n"
        "FORMAT CSV SEPARATOR 'separator' \n"
        "PREFIX '2017\\t2\\t1\\t' \n"
        "QUERY 'select * from tutu where custom = \\'aa\\'' \n"
        "SKIP 2 \n"
        "NOCHECK \n"
        "LIMIT 1000 \n"
        "LOCALE 'fr';"
    )
