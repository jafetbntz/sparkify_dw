import pandas as pd


def __redshift_props(redshift, cluster_identifier):
    props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    if props == None:
        return None
    pd.set_option('display.max_colwidth', 500)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])