import boto3
import logging

from datetime import datetime

from sparkflowtools.models import instance_fleets
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

    def add_log_uri(self, log_uri_location: str):
        """

        :param log_uri_location:
        :return:
        """
        assert s3_path_utils.is_valid_s3_path(log_uri_location)
        self.log_uri = log_uri_location
        return self

    def add_emr_release(self, emr_release_label: str):
        """

        :param emr_release_label:
        :return:
        """
        # TODO ensure this is a valid/supported EMR release
        self.emr_release_label = emr_release_label
        return self

    def add_applications(self, applications: list):
        """

        :param applications:
        :return:
        """
        # TODO ensure these are valid applications that can be run
        self.applications = [{"Name": application["Name"]} for application in applications]
        return self

    def add_ec2_attributes(self, ec2_attributes: dict):
        """

        :param ec2_attributes:
        :return:
        """
        self.ec2_key_name = ec2_attributes["Ec2KeyName"]
        self.ec2_subnet_id = ec2_attributes["Ec2SubnetId"]
        self.ec2_availability_zone = ec2_attributes["Ec2AvailabilityZone"]
        self.ec2_role = ec2_attributes["IamInstanceProfile"]
        self.driver_security_group = ec2_attributes["EmrManagedMasterSecurityGroup"]
        self.worker_security_group = ec2_attributes["EmrManagedSlaveSecurityGroup"]
        return self

    def add_termination_behavior(self, auto_terminate: bool, termination_protection: bool):
        """

        :param auto_terminate:
        :param termination_protection:
        :return:
        """
        self.auto_terminate = auto_terminate
        self.termination_protected = termination_protection
        return self

    def add_instance_fleet(self, fleet_type: str):
        """

        :param fleet_type:
        :return:
        """
        self.fleet_type = fleet_type
        self.instance_fleets = instance_fleets.get_fleet(fleet_type).get_fleet()
        return self

    def add_tags(self, tags: list):
        """

        :param tags:
        :return:
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
        """

        :param configurations:
        :return:
        """
        self.configurations = configurations
        return self

    def add_service_role(self, service_role: str):
        """

        :param service_role:
        :return:
        """
        self.emr_role = service_role
        return self

    def launch(self, client: boto3.client = None) -> str:
        """

        :param client:
        :return:
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
        try:
            logging.info("Launching {0} cluster with name {1} and ID {2}".format(
                self.fleet_type, self.name, self.cluster_id))
            print(job_run_flow)
            self.cluster_id = emr.create_cluster(job_run_flow, client=client)["cluster_id"]
        except Exception as e:
            logging.critical("cluster.EmrCluster.launch could not create a new EMR cluster")
            logging.exception(e)
        return self.cluster_id

    def terminate(self, client: boto3.client = None) -> None:
        """

        :param client:
        :return:
        """
        pass


class EmrBuilder(object):

    @staticmethod
    def get_fleet_type_from_tags(tags: list) -> str:
        """

        :param tags:
        :return:
        """
        fleet_type = None
        for tag in tags:
            if tag["Key"] == "fleet_type":
                fleet_type = tag["Value"]
        return fleet_type

    def _create_cluster_from_cluster_info(self, cluster: EmrCluster, cluster_info: dict, client: boto3.client = None):
        """

        :param cluster:
        :param cluster_info:
        :param client:
        :return:
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
        cluster.launch(client=client)

    def build_from_existing_cluster(self, cluster_id: str, name: str = None, client: boto3.client = None) -> EmrCluster:
        """

        :param cluster_id:
        :param name:
        :param client:
        :return:
        """
        cluster_info = emr.get_cluster_info(cluster_id, client=client)
        # If a name is not provided through arguments then create a derived name from the given cluster's name
        if not name:
            name = cluster_info["Name"] + datetime.now().strftime("%Y-%m-%dT%H%M%S")
        cluster = EmrCluster(name)
        self._create_cluster_from_cluster_info(cluster, cluster_info, client=client)
        return cluster

    def build_from_config(self, config: dict) -> EmrCluster:
        pass
