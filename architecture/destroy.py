
import boto3
import configparser
from common import __redshift_props

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
IAM_ROLE_NAME      = config.get("IAM_ROLE", "ARN")

DW_REGION_NAME         = config.get("DWH", "REGION_NAME")          
DWH_CLUSTER_IDENTIFIER = config.get("DWH","CLUSTER_IDENTIFIER")






def main():


    redshift = boto3.client('redshift',
                    region_name=DW_REGION_NAME,
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )


    iam = boto3.client('iam',aws_access_key_id=KEY,
                aws_secret_access_key=SECRET,
                region_name=DW_REGION_NAME
            )

    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

    props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    __redshift_props(props, DWH_CLUSTER_IDENTIFIER)


    iam.detach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=IAM_ROLE_NAME)



if __name__ == "__main__":
    main()