
"""
Using Papermill to run a Jupyter-notebook
using Airflow
"""


from typing import Dict
from datetime import datetime

import papermill as pm

from airflow import DAG
from airflow.models import BaseOperator
from airflow.lineage.datasets import DataSet
from airflow.utils.decorators import apply_defaults



class NoteBook(DataSet):
    type_name = "jupyter_notebook"
    attributes = ['location', 'parameters']


class PapermillOperator(BaseOperator):
    """
    Executes a jupyter notebook through papermill that is annotated with parameters
    :param input_nb: input notebook (can also be a NoteBook or a File inlet)
    :type input_nb: str
    :param output_nb: output notebook (can also be a NoteBook or File outlet)
    :type output_nb: str
    :param parameters: the notebook parameters to set
    :type parameters: dict
    """
    @apply_defaults
    def __init__(self,
                 input_nb: str,
                 output_nb: str,
                 parameters: Dict,
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.inlets.append(NoteBook(qualified_name=input_nb,
                                    location=input_nb,
                                    parameters=parameters))
        self.outlets.append(NoteBook(qualified_name=output_nb,
                                     location=output_nb))

    def execute(self, context):
        for i in range(len(self.inlets)):
            pm.execute_notebook(self.inlets[i].location, self.outlets[i].location,
                                parameters=self.inlets[i].parameters,
                                progress_bar=False, report_mode=True)



dag = DAG(
    'papermill_example',
    description='Running a Notebook',
    schedule_interval='0 12 * * *',
    start_date=datetime(2017, 3, 20),
    catchup=False
)


run_this = PapermillOperator(
    task_id="run_example_notebook",
    dag=dag,
    input_nb='gs://insr-notebooks-storage/example.ipynb',
    output_nb='gs://insr-notebooks-storage/example.ipynb',
    parameters={"a": 120}
)