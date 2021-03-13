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
        self.script_path = "command-runner.jar"
        self.spark_args = {}
        self.job_args = {}
        self.job_class = None
        self.job_jar = None
        self.args = None

    @staticmethod
    def parse_input_args(args: dict) -> list:
        """Parses a dictionary of key value pairs for argument parameters

        :param args a dictionary of key value parameters to pass
        :returns a list of lists where each list contains the key value pairs
        """
        return [["{0}".format(arg), "{0}".format(value)] for arg, value in args.items()]

    def _validate_args(self):
        """Validates the instance attributes before constructing the step payload"""
        if not self.job_class:
            raise ValueError("value {0} is not a valid main class".format(self.job_class))
        elif not self.job_jar:
            raise ValueError("value {0} is not a valid JAR to execute with spark-submit".format(self.job_jar))

    def _populate_spark_submit_args(self):
        """Constructs the spark-submit command to provide as the args input to a HadoopJarStep on EMR"""
        self._validate_args()
        self.args = ['spark-submit', '--deploy-mode', 'cluster']
        class_input = ['--class', self.job_class]
        spark_args = self.parse_input_args(self.spark_args)
        user_args = self.parse_input_args(self.job_args)
        for arg in spark_args:
            self.args.extend(arg)
        self.args.extend(class_input)
        self.args.append(self.job_jar)
        for arg in user_args:
            self.args.extend(arg)

    def _construct_step_payload(self) -> dict:
        """Provides the step payload expected by EMR from the attached attributes

        :return: a dictionary containing the EMR step payload attributes required to launch a job on a cluster
        """
        self._populate_spark_submit_args()
        return {
            'Name': self.name,
            'ActionOnFailure': self.action_on_failure,
            'HadoopJarStep': {
                'Jar': self.script_path,
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
