import glob
import importlib.util
import os

import pytest
from airflow.models import DAG, DagBag
from airflow.utils.dag_cycle_tester import test_cycle as _test_cycle

a = DagBag()


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(include_examples=False)


def test_dags_load_with_no_errors(dagbag):
    assert len(dagbag.import_errors) == 0


def test_task_count(dagbag):
    DAG_ID = "coins_etl_dag"
    TASK_COUNT = 4

    dag = dagbag.get_dag(DAG_ID)
    assert len(dag.tasks) == TASK_COUNT


def test_time_import_dags(dagbag):
    LOAD_SECOND_THRESHOLD = 1

    stats = dagbag.dagbag_stats

    slow_dags = list(
        filter(lambda x: x.duration.total_seconds() > LOAD_SECOND_THRESHOLD, stats)
    )

    assert len(slow_dags) == 0


DAG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "dags/**/*.py")
DAG_FILES = glob.glob(DAG_PATH, recursive=True)


@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_integrity(dag_file, dagbag):
    module_name, _ = os.path.splitext(dag_file)  # Load file
    module_path = os.path.join(DAG_PATH, dag_file)  # Extract DAG Objects

    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(mod_spec)

    mod_spec.loader.exec_module(module)
    dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]
    # dag_objects = [dag for key, dag in dagbag.dags.items()]

    assert dag_objects

    for dag in dag_objects:
        # Validate that there is no cycle for each DAG
        _test_cycle(dag)
