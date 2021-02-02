from sparkflowtools.models import cluster


class EmrMonitor(object):

    def __init__(self, config: dict):
        self.config = config

    def _get_steps(self) -> dict:
        pass

    def _refresh_step_statuses(self) -> None:
        pass

    def _refresh_cluster_statuses(self) -> None:
        pass

    def add_cluster(self, cluster_to_add: cluster.EmrCluster) -> None:
        pass

    def get_available_clusters(self):
        pass

    def refresh(self):
        self._refresh_step_statuses()
        self._refresh_cluster_statuses()
