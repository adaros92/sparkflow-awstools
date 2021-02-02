from sparkflowtools.models import monitor, cluster

emr_monitor = monitor.EmrMonitor({})


def test_get_steps():
    emr_monitor._get_steps()


def test_refresh_step_statuses():
    emr_monitor._refresh_step_statuses()


def test_refresh_cluster_statuses():
    emr_monitor._refresh_cluster_statuses()


def test_add_cluster():
    emr_cluster = cluster.EmrCluster("some_cluster")
    emr_monitor.add_cluster(emr_cluster)


def test_get_available_clusters():
    emr_monitor.get_available_clusters()


def test_refresh():
    emr_monitor.refresh()
