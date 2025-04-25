from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

cassandra_user = ""
cassandra_password = ""


def get_cassandra_session():
    auth_provider = PlainTextAuthProvider(
        cassandra_user, cassandra_password
    )  # Update credentials
    # 127.0.0.1
    cluster = Cluster(
        ["127.0.0.1"], auth_provider=auth_provider
    )  # Update with actual IP
    session = cluster.connect()
    session.set_keyspace("datasnake")
    return session
