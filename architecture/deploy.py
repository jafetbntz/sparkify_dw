import pandas as pd
import boto3
import json
import configparser
from common import __redshift_props


config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","NODE_TYPE")
DW_REGION_NAME         = config.get("DWH", "REGION_NAME")          
DWH_CLUSTER_IDENTIFIER = config.get("DWH","CLUSTER_IDENTIFIER")

DWH_DB                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DB_PORT")


IAM_ROLE_NAME      = config.get("IAM_ROLE", "ID")




def create_role():
    iam = boto3.client('iam',aws_access_key_id=KEY,
                aws_secret_access_key=SECRET,
                region_name=DW_REGION_NAME
            )

    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        
        
    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)
    return roleArn

def create_cluster(redshift, roleArn):
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)





def main():


    redshift = boto3.client('redshift',
                    region_name=DW_REGION_NAME,
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

    props = pd.DataFrame() 
    try:
        props = __redshift_props(redshift, DWH_CLUSTER_IDENTIFIER)
    except Exception as e:
        print(e)

    if props.empty:

        print("Cluster not found. Starting creation...")
        role = create_role()
        print(role)
        if role != None:
             print("Role has been created...")

        cluster = create_cluster(redshift, role)

        if cluster != None:
             print("Cluster has been created...")

        props = __redshift_props(redshift, DWH_CLUSTER_IDENTIFIER)
    print(props)

    DWH_ENDPOINT =  props['Endpoint']['Address']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)    
    
    # DWH_ROLE_ARN = props['IamRoles'][0]['IamRoleArn']


    # print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    if DWH_ENDPOINT != None:
        config.set("CLUSTER", "HOST", DWH_ENDPOINT)

    # if DWH_ROLE_ARN != None:
    #     config.set("IAM_ROLE", "ARN", DWH_ROLE_ARN)


if __name__ == "__main__":
    main()