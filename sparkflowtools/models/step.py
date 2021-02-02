import boto3
import logging

from sparkflowtools.utils import emr


class EmrStep(object):

    def __init__(self, name: str):
        self.name = name
        self.step_id = None
        self.step_status = {}
        self.action_on_failure = "TERMINATE_CLUSTER"
        self.cluster_id = None
        self.script_path = None
        self.main_class = None
        self.args = []

    def _construct_step_payload(self) -> dict:
        """Provides the step payload expected by EMR from the attached attributes

        :return: a dictionary containing the EMR step payload attributes required to launch a job on a cluster
        """
        return {
            'Name': 'string',
            'ActionOnFailure': self.action_on_failure,
            'HadoopJarStep': {
                'Jar': self.script_path,
                'MainClass': self.main_class,
                'Args': self.args
            }
        }

    def assign_to_cluster(self, cluster_id: str, step_id: str, client: boto3.client = None):
        """Assigns the current step to to a given cluster and keeps track of the step ID of the step on that
        cluster

        :param cluster_id: the ID of the cluster that the step is running on
        :param step_id: the ID of the step on that cluster
        :param client: an optional boto3 client to use for the request
        :return: a reference to this instance
        """
        self.cluster_id = cluster_id
        self.step_id = step_id
        self.fetch_status(client=client)
        return self

    def add_script(self, script_path: str = "command-runner.jar", main_class: str = None):
        """Attaches the main driver class for the Spark application and the script path pointing to the script
        that needs to be run

        :param main_class: the location of the driver class within the package being run
        :param script_path: the path of the script to run with this step
        :return: a reference to this instance
        """
        self.script_path = script_path
        self.main_class = main_class
        return self

    def fetch_status(self, client: boto3.client = None) -> dict:
        """Retrieves the status of the job

        :param client: an optional boto3 client to use for the request
        :return: a dictionary containing information about the step's status on EMR
        """
        if not self.step_id or not self.cluster_id:
            logging.warning("Step {0} is not running on any cluster".format(self.name))
            return {}
        self.step_status = emr.get_step_status(self.cluster_id, self.step_id, client=client)
        return self.step_status

    @property
    def payload(self) -> dict:
        """Retrieves the step payload with attributes required by EMR for the step to be successfully launched

        :return: a dictionary containing the payload parameters
        """
        return self._construct_step_payload()
