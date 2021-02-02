import boto3
import logging

from datetime import datetime

from sparkflowtools.models import instance_fleets, step
from sparkflowtools.utils import emr, s3_path_utils


class EmrCluster(object):

    def __init__(self, name):
        self.name = name
        self.cluster_id = None
        self.state = 'Starting'
        self.log_uri = None
        self.emr_release_label = "emr-5.32.0"
        self.applications = [
            {'Name': 'Spark'}, {'Name': 'Zeppelin'}
        ]
        self.ec2_key_name = None
        self.ec2_subnet_id = None
        self.ec2_availability_zone = None
        self.ec2_role = "EMR_EC2_DefaultRole"
        self.emr_role = "EMR_DefaultRole"
        self.driver_security_group = None
        self.worker_security_group = None
        self.auto_terminate = False
        self.termination_protected = False
        self.fleet_type = "nano"
        self.instance_fleets = instance_fleets.get_fleet(self.fleet_type).get_fleet()
        self.bootstrap_actions = []
        self.visible_to_all_users = True
        self.configurations = []
        self.tags = [{"Key": "fleet_type", "Value": self.fleet_type}]
        self.running_steps = {}

    def add_log_uri(self, log_uri_location: str):
        """Attaches the S3 location to save logs to

        :param log_uri_location: a path in S3 to save logs to
        :return: a reference to this instance
        """
        assert s3_path_utils.is_valid_s3_path(log_uri_location)
        self.log_uri = log_uri_location
        return self

    def add_emr_release(self, emr_release_label: str):
        """Attaches the EMR release label to the EMR

        :param emr_release_label: a release label for the EMR version to use
        :return: a reference to this instance
        """
        # TODO ensure this is a valid/supported EMR release
        self.emr_release_label = emr_release_label
        return self

    def add_applications(self, applications: list):
        """Attaches a list of applications to create the cluster with

        :param applications: a list of applications to run on EMR (Spark, Zeppelin, etc.)
        :return: a reference to this instance
        """
        # TODO ensure these are valid applications that can be run
        self.applications = [{"Name": application["Name"]} for application in applications]
        return self

    def add_ec2_attributes(self, ec2_attributes: dict):
        """Attaches information about the EC2 and EMR roles, subnets, and availability zones to use

        :param ec2_attributes: a dictionary containing EC2/EMR role, subnet, availability zone info
        :return: a reference to this instance
        """
        self.ec2_key_name = ec2_attributes["Ec2KeyName"]
        self.ec2_subnet_id = ec2_attributes["Ec2SubnetId"]
        self.ec2_availability_zone = ec2_attributes["Ec2AvailabilityZone"]
        self.ec2_role = ec2_attributes["IamInstanceProfile"]
        self.driver_security_group = ec2_attributes["EmrManagedMasterSecurityGroup"]
        self.worker_security_group = ec2_attributes["EmrManagedSlaveSecurityGroup"]
        return self

    def add_termination_behavior(self, auto_terminate: bool, termination_protection: bool):
        """Attaches the termination behavior of the cluster dictating whether to terminate when no steps have
        run and whether to protect from manual termination

        :param auto_terminate: a boolean denoting whether to keep the cluster alive in case there are no steps
        :param termination_protection: a boolean denoting whether to protect from manual termination
        :return: a reference to this instance
        """
        self.auto_terminate = auto_terminate
        self.termination_protected = termination_protection
        return self

    def add_instance_fleet(self, fleet_type: str):
        """Attaches the type of fleet from the available instance fleet types

        :param fleet_type: the name of the fleet type to retrieve and attach
        :return: a reference to this instance
        """
        self.fleet_type = fleet_type
        self.instance_fleets = instance_fleets.get_fleet(fleet_type).get_fleet()
        return self

    def add_tags(self, tags: list):
        """Attaches a list of tags to to the cluster

        :param tags: a list of tag strings to attach
        :return: a reference to this instance
        """
        # Get rid of any existing tags that match those in provided list
        for tag in tags:
            tag_key = tag["Key"]
            for idx, registered_tag in enumerate(self.tags):
                if tag_key == registered_tag["Key"]:
                    self.tags.pop(idx)
        self.tags.extend(tags)
        return self

    def add_configurations(self, configurations: list):
        """Attaches cluster-wide configurations

        :param configurations: a list of configuration inputs to use
        :return: a reference to this instance
        """
        self.configurations = configurations
        return self

    def add_service_role(self, service_role: str):
        """Attaches the EMR IAM role to the cluster

        :param service_role: the EMR IAM role to use with this cluster
        :return: a reference to this instance
        """
        self.emr_role = service_role
        return self

    def launch(self, client: boto3.client = None) -> str:
        """Creates a new EMR cluster with the current attributes

        :param client: an optional boto3 client to use for the creation request
        :return: the cluster_id returned from the cluster creation AWS endpoint
        """
        job_run_flow = {
            "Name": self.name, "LogUri": self.log_uri, "ReleaseLabel": self.emr_release_label,
            "Instances":
                {
                    "InstanceFleets": self.instance_fleets,
                    "Ec2KeyName": self.ec2_key_name,
                    "KeepJobFlowAliveWhenNoSteps": self.auto_terminate,
                    "TerminationProtected": self.termination_protected,
                    "Ec2SubnetIds": [self.ec2_subnet_id]
                },
            "BootstrapActions": self.bootstrap_actions,
            "Applications": self.applications,
            "Configurations": self.configurations,
            "VisibleToAllUsers": self.visible_to_all_users,
            "JobFlowRole": self.ec2_role,
            "ServiceRole": self.emr_role,
            "Tags": self.tags
        }
        logging.info("Launching {0} cluster with name {1} and ID {2}".format(
            self.fleet_type, self.name, self.cluster_id))
        try:
            self.cluster_id = emr.create_cluster(job_run_flow, client=client)["cluster_id"]
        except Exception as e:
            logging.critical("cluster.EmrCluster.launch could not create a new EMR cluster")
            logging.exception(e)
        return self.cluster_id

    def terminate(self, client: boto3.client = None) -> None:
        """Shuts down the current cluster

        :param client: an optional boto3 client to use for the creation request
        :return:
        """
        logging.info("Terminating {0} cluster with name {1} and ID {2}".format(
            self.fleet_type, self.name, self.cluster_id
        ))
        try:
            emr.terminate_clusters([self.cluster_id], client=client)
        except Exception as e:
            logging.critical("cluster.EmrCluster.terminate could not tear down cluster {0}".format(self.cluster_id))
            logging.exception(e)

    def submit_step(self, emr_step: step.EmrStep, client: boto3.client = None) -> None:
        """Submits a Spark step/job on the current EMR cluster

        :param emr_step: a step object to retrieve the required job run flow information from
        :param client: an optional boto3 client to use for the creation request
        """
        if not self.cluster_id:
            raise RuntimeError("Cannot submit a step to a cluster that is not running; launch cluster first")
        payload = emr_step.payload
        try:
            response = emr.submit_step(self.cluster_id, [payload], client=client)
            step_id = response["StepIds"][0]
            emr_step.assign_to_cluster(self.cluster_id, step_id)
            self.running_steps[step_id] = emr_step
        except Exception as e:
            logging.critical("cluster.EmrCluster.submit_step could not submit step {0} to cluster {1}".format(
                payload, self.cluster_id
            ))
            logging.exception(e)


class EmrBuilder(object):

    @staticmethod
    def get_fleet_type_from_tags(tags: list) -> str:
        """Utility method used to retrieve the type of the fleet from a list of EMR AWS tags

        :param tags: a list of tags to retrieve the instance fleet type from
        :return: the name of the instance fleet type as defined in instance_fleets
        """
        fleet_type = None
        for tag in tags:
            if tag["Key"] == "fleet_type":
                fleet_type = tag["Value"]
        return fleet_type

    def _create_cluster_from_cluster_info(self,
                                          cluster: EmrCluster, cluster_info: dict,
                                          client: boto3.client = None) -> str:
        """Creates a new EMR cluster from a given dictionary containing expected parameters

        :param cluster: an EmrCluster object to build from the given cluster_info dictionary
        :param cluster_info: a dictionary containing the parameters necessary to launch an EMR cluster with
        :param client: an optional boto3 client to use for the request
        :return: the cluster_id of the newly launched EMR cluster
        """
        # Get the fleet_type from the given cluster's tags
        tags = cluster_info["Tags"]
        try:
            fleet_type = self.get_fleet_type_from_tags(tags)
            instance_fleets.get_fleet(fleet_type)
        # If not available then just assume the default fleet_type
        except ValueError:
            logging.info("cluster.EmrBuilder can't use fleet type from cluster tags to build new cluster")
            fleet_type = cluster.fleet_type
        # Build a new cluster with parameters from the given cluster and launch it
        cluster.add_log_uri(cluster_info["LogUri"]).add_emr_release(cluster_info["ReleaseLabel"])\
            .add_applications(cluster_info["Applications"]).add_ec2_attributes(cluster_info["Ec2InstanceAttributes"])\
            .add_instance_fleet(fleet_type).add_tags(cluster_info["Tags"])\
            .add_termination_behavior(cluster_info["AutoTerminate"], cluster_info["TerminationProtected"])\
            .add_configurations(cluster_info["Configurations"]).add_service_role(cluster_info["ServiceRole"])
        return cluster.launch(client=client)

    def build_from_config(self, config: dict, name: str = None, client: boto3.client = None) -> EmrCluster:
        """Builds a new EMR cluster from a given dictionary config containing all the necessary parameters
        expected by AWS EMR API

        :param config: a dictionary containing the parameters to create a new EMR cluster from
        :param name: an optional name to give the new cluster if one is not already provided through config
        :param client: an optional boto3 client to use for the request
        :return: An EmrCluster object containing information for the newly created cluster
        """
        if not name:
            name = config.get("Name", "") + datetime.now().strftime("%Y-%m-%dT%H%M%S")
        cluster = EmrCluster(name)
        self._create_cluster_from_cluster_info(cluster, config, client=client)
        return cluster

    def build_from_existing_cluster(self, cluster_id: str, name: str = None, client: boto3.client = None) -> EmrCluster:
        """Builds a new EMR cluster from an existing EMR cluster in AWS

        :param cluster_id: the ID of the cluster to build a new one from
        :param name: an optional name to give the new cluster
        :param client: an optional boto3 client to use for the request
        :return: An EmrCluster object containing information for the newly created cluster
        """
        cluster_info = emr.get_cluster_info(cluster_id, client=client)
        cluster = self.build_from_config(cluster_info, name, client)
        return cluster
