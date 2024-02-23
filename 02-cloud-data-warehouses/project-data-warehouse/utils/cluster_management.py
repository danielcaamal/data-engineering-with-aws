import boto3
import configparser
import json
import pandas as pd
from get_args import get_arguments

class RedshiftCluster:
  redshift: any = None
  
  def __init__(self, key:str, secret:str, region_name: str) -> None:
    self.init_resource(key=key, secret=secret, region_name=region_name)
  
  
  def init_resource(self, key:str, secret:str, region_name="us-west-2"):
    try:
      self.redshift = boto3.client(
        'redshift', 
        region_name=region_name, 
        aws_access_key_id=key,
        aws_secret_access_key=secret
      )
    except Exception as e:
      print("Error initializing the redshift resource: ", e)
  
  
  def get_redshift_cluster(self, cluster_identifier: str) -> str:
    """Get the redshift cluster
    
    Args:
        cluster_identifier (str): The identifier of the cluster
        
    Returns:
        str: The redshift cluster properties
    """
    try:
      myClusterProps = self.redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
      self.prettyRedshiftProps(myClusterProps)
      if myClusterProps['ClusterStatus'] == 'available':
        print("The cluster is available")
        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
        print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
        print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
      return myClusterProps
    except Exception as e:
      print("Error getting the redshift cluster: ", e)
  
  
  def delete_redshift_cluster(self, cluster_identifier: str) -> None:
    """Delete the redshift cluster

    Args:
        cluster_identifier (str): The identifier of the cluster
    """
    try:
      self.redshift.delete_cluster( ClusterIdentifier=cluster_identifier,  SkipFinalClusterSnapshot=True)
    except Exception as e:
      print("Error deleting the redshift cluster: ", e)
  
  
  def create_redshift_cluster(self, 
      cluster_type: str,
      node_type: str,
      num_nodes: int,
      cluster_identifier: str,
      db_name: str,
      db_user: str,
      db_password: str,
      db_port: int,
      role_arn: str
    ) -> None:
    """Create the redshift cluster
    
    Args:
        cluster_type (str): The type of the cluster
        node_type (str): The type of the node
        num_nodes (int): The number of nodes
        cluster_identifier (str): The identifier of the cluster
        db_name (str): The name of the database
        db_user (str): The user of the database
        db_password (str): The password of the database
        db_port (int): The port of the database
        role_arn (str): The role arn
    """
    try:
      self.redshift.create_cluster(        
        # Parameters for hardware
        ClusterType=cluster_type,
        NodeType=node_type,
        NumberOfNodes=int(num_nodes),
        
        # Parameters for identifiers & credentials
        DBName=db_name,
        ClusterIdentifier=cluster_identifier,
        MasterUsername=db_user,
        MasterUserPassword=db_password,
        Port=int(db_port),
        
        # Parameters for role (to allow s3 access)
        IamRoles=[role_arn]  
    )
    except Exception as e:
      print("Error creating the role: ", e)
  
  
  def prettyRedshiftProps(self, props):
    """Print the redshift properties

    Args:
        props (dict): The properties of the redshift
    """
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    print(pd.DataFrame(data=x, columns=["Key", "Value"]))


def read_config_file(file_name='dwh.cfg') -> dict:
  """Read the configuration file

  Args:
      file_name (str, optional): The file name to read the configuration from. Defaults to 'dwh.cfg'.

  Returns:
      dict: The configuration values
  """
  config = configparser.ConfigParser()
  config.read_file(open(file_name))
  KEY                     = config.get('IAM_MANAGEMENT','IAM_KEY')
  SECRET                  = config.get('IAM_MANAGEMENT','IAM_SECRET')
  DWH_ROLE_ARN            = config.get('CLUSTER_MANAGEMENT','DWH_ROLE_ARN')
  DWH_CLUSTER_TYPE        = config.get("CLUSTER_MANAGEMENT","DWH_CLUSTER_TYPE")
  DWH_NUM_NODES           = config.get("CLUSTER_MANAGEMENT","DWH_NUM_NODES")
  DWH_NODE_TYPE           = config.get("CLUSTER_MANAGEMENT","DWH_NODE_TYPE")
  DWH_CLUSTER_IDENTIFIER  = config.get("CLUSTER_MANAGEMENT","DWH_CLUSTER_IDENTIFIER")
  DWH_DB                  = config.get("CLUSTER_MANAGEMENT","DWH_DB")
  DWH_DB_USER             = config.get("CLUSTER_MANAGEMENT","DWH_DB_USER")
  DWH_DB_PASSWORD         = config.get("CLUSTER_MANAGEMENT","DWH_DB_PASSWORD")
  DWH_PORT                = config.get("CLUSTER_MANAGEMENT","DWH_PORT")
  DWH_REGION              = config.get("CLUSTER_MANAGEMENT","DWH_REGION")
  
  return {
    "KEY": KEY,
    "SECRET": SECRET,
    "DWH_ROLE_ARN": DWH_ROLE_ARN,
    "DWH_CLUSTER_TYPE": DWH_CLUSTER_TYPE,
    "DWH_NUM_NODES": DWH_NUM_NODES,
    "DWH_NODE_TYPE": DWH_NODE_TYPE,
    "DWH_CLUSTER_IDENTIFIER": DWH_CLUSTER_IDENTIFIER,
    "DWH_DB": DWH_DB,
    "DWH_DB_USER": DWH_DB_USER,
    "DWH_DB_PASSWORD": DWH_DB_PASSWORD,
    "DWH_PORT": DWH_PORT,
    "DWH_REGION": DWH_REGION
  }
  
  
def main():
  """Allow the user to get, create or delete the redshift cluster following the requirements for 
  the etl.py script
  """
  # Read the config file
  config = read_config_file()
  args = get_arguments()
  
  # Initialize the redshift cluster class
  redshift_cluster = RedshiftCluster(key=config['KEY'], secret=config['SECRET'], region_name=config["DWH_REGION"])
  
  # Uncomment or comment for delete the cluster
  if args.option == 'delete':
    redshift_cluster.delete_redshift_cluster(
      cluster_identifier=config['DWH_CLUSTER_IDENTIFIER']
    )
  
  # Create the cluster
  if args.option == 'create':
    redshift_cluster.create_redshift_cluster(
      cluster_type=config['DWH_CLUSTER_TYPE'],
      node_type=config['DWH_NODE_TYPE'],
      num_nodes=config['DWH_NUM_NODES'],
      cluster_identifier=config['DWH_CLUSTER_IDENTIFIER'],
      db_name=config['DWH_DB'],
      db_user=config['DWH_DB_USER'],
      db_password=config['DWH_DB_PASSWORD'],
      db_port=config['DWH_PORT'],
      role_arn=config['DWH_ROLE_ARN']
    )
  
  # Default the get method to get the cluster
  redshift_cluster = redshift_cluster.get_redshift_cluster(
    cluster_identifier=config['DWH_CLUSTER_IDENTIFIER']
  )

if __name__ == "__main__":
  main()