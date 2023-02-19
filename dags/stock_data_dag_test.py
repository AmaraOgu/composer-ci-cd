
from airflow.models import DagBag

def test_dagbag_import():
    '''Tests there are no syntax issues or environment compaibility issues
    '''
    dagbag = DagBag()
    assert not dagbag.import_errors, "Found errors in DAG import:\n{}".format(dagbag.import_errors)

def test_no_cycles_in_dag():
    '''Tests there are no cycles
    '''
    dagbag = DagBag()
    for dag_id, dag in dagbag.dags.items():
        assert not dag.test_cycle(), f"Found a cycle in DAG '{dag_id}'"

def test_dag_owners():
    '''Tests that owners are set for all dags and it is not "airflow"
    '''
    dagbag = DagBag()
    for dag_id, dag in dagbag.dags.items():
        assert dag.owner != None and dag.owner != 'airflow', f"DAG '{dag_id}' does not have a valid owner"

def test_dag_tags():
    '''Test that the they have a tag
    '''
    dagbag = DagBag()
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags != None and 'example' in dag.tags, f"DAG '{dag_id}' does not have a valid tag"