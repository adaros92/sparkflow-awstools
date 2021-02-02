import pytest

from datetime import datetime


class EmrClient(object):
    """Mocks boto3.client('emr')"""

    @staticmethod
    def describe_step(**kwargs):
        return {
            'Step': {
                'Id': 'some_id',
                'Name': 'some_job',
                'Config': {
                    'Jar': 'some_jar',
                    'Properties': {
                        'some_property_name': 'some_property_value'
                    },
                    'MainClass': 'some_class',
                    'Args': [
                        'some_arg',
                    ]
                },
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'Status': {
                    'State': 'RUNNING',
                    'StateChangeReason': {
                        'Code': 'some_code',
                        'Message': 'some_message'
                    },
                    'FailureDetails': {
                        'Reason': 'some_reason',
                        'Message': 'some_message',
                        'LogFile': 'some_log_file'
                    },
                    'Timeline': {
                        'CreationDateTime': datetime(2021, 1, 1),
                        'StartDateTime': datetime(2021, 1, 1),
                        'EndDateTime': datetime(2021, 1, 1)
                    }
                }
            }
        }

    @staticmethod
    def add_job_flow_steps(**kwargs):
        return {
            'StepIds': [
                'some_id',
            ]
        }

    @staticmethod
    def run_job_flow(**kwargs):
        return {
            'JobFlowId': 'some_cluster_id',
            'ClusterArn': 'some_cluster_arn'
        }

    @staticmethod
    def describe_cluster(**kwargs):
        return {
            'Cluster': {
                'Id': 'string',
                'Name': 'string',
                'Status': {
                    'State': 'STARTING',
                    'StateChangeReason': {
                        'Code': 'INTERNAL_ERROR',
                        'Message': 'string'
                    },
                    'Timeline': {
                        'CreationDateTime': datetime(2015, 1, 1),
                        'ReadyDateTime': datetime(2015, 1, 1),
                        'EndDateTime': datetime(2015, 1, 1)
                    }
                },
                'Ec2InstanceAttributes': {
                    'Ec2KeyName': 'string',
                    'Ec2SubnetId': 'string',
                    'RequestedEc2SubnetIds': [
                        'string',
                    ],
                    'Ec2AvailabilityZone': 'string',
                    'RequestedEc2AvailabilityZones': [
                        'string',
                    ],
                    'IamInstanceProfile': 'string',
                    'EmrManagedMasterSecurityGroup': 'string',
                    'EmrManagedSlaveSecurityGroup': 'string',
                    'ServiceAccessSecurityGroup': 'string',
                    'AdditionalMasterSecurityGroups': [
                        'string',
                    ],
                    'AdditionalSlaveSecurityGroups': [
                        'string',
                    ]
                },
                'InstanceCollectionType': 'INSTANCE_FLEET',
                'LogUri': 'string',
                'LogEncryptionKmsKeyId': 'string',
                'RequestedAmiVersion': 'string',
                'RunningAmiVersion': 'string',
                'ReleaseLabel': 'string',
                'AutoTerminate': True,
                'TerminationProtected': True,
                'VisibleToAllUsers': True,
                'Applications': [
                    {
                        'Name': 'string',
                        'Version': 'string',
                        'Args': [
                            'string',
                        ],
                        'AdditionalInfo': {
                            'string': 'string'
                        }
                    },
                ],
                'Tags': [
                    {
                        'Key': 'string',
                        'Value': 'string'
                    },
                ],
                'ServiceRole': 'string',
                'NormalizedInstanceHours': 123,
                'MasterPublicDnsName': 'string',
                'Configurations': [
                    {
                        'Classification': 'string',
                        'Configurations': {'... recursive ...'},
                        'Properties': {
                            'string': 'string'
                        }
                    },
                ],
                'SecurityConfiguration': 'string',
                'AutoScalingRole': 'string',
                'ScaleDownBehavior': 'TERMINATE_AT_INSTANCE_HOUR',
                'CustomAmiId': 'string',
                'EbsRootVolumeSize': 123,
                'RepoUpgradeOnBoot': 'SECURITY',
                'KerberosAttributes': {
                    'Realm': 'string',
                    'KdcAdminPassword': 'string',
                    'CrossRealmTrustPrincipalPassword': 'string',
                    'ADDomainJoinUser': 'string',
                    'ADDomainJoinPassword': 'string'
                },
                'ClusterArn': 'string',
                'OutpostArn': 'string',
                'StepConcurrencyLevel': 123,
                'PlacementGroups': [
                    {
                        'InstanceRole': 'MASTER',
                        'PlacementStrategy': 'SPREAD'
                    },
                ]
            }
        }

    @staticmethod
    def terminate_job_flows(**kwargs):
        return None


def pytest_configure():
    """Configures universal pytest parameters for running unit tests"""
    pytest.mock_emr_client = EmrClient()
