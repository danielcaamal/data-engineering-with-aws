import boto3
import configparser
import json
import pandas as pd
from get_args import get_arguments

class IamRole:
  iam: any = None
  
  def __init__(self, key:str, secret:str, region_name: str) -> None:
    self.init_resource(key=key, secret=secret, region_name=region_name)
  
  
  def init_resource(self, key:str, secret:str, region_name="us-west-2"):
    try:
      self.iam = boto3.client(
        'iam', 
        region_name=region_name,
        aws_access_key_id=key,
        aws_secret_access_key=secret
      )
    except Exception as e:
      print("Error initializing the Iam resource: ", e)
  
  
  def get_iam_role(self, role_name: str) -> str:
    """Get the IAM role

    Args:
        role_name (str): The role name

    Returns:
        str: The role ARN
    """
    try:
      roleArn = self.iam.get_role(
        RoleName=role_name
      )['Role']['Arn']
      return roleArn
    except Exception as e:
      print("Role not found")
  
  
  def delete_iam_role(self, role_name: str) -> None:
    """Delete the IAM role and the role policy

    Args:
        role_name (str): The role name
    """
    try:
      self.iam.detach_role_policy(
        RoleName=role_name, 
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
      )
    except Exception as e:
      print("Error deleting the role policy: ", e)
    
    try:
      self.iam.delete_role(
        RoleName=role_name
      )
    except Exception as e:
      print("Error deleting the role: ", e)
  
  
  def create_iam_role(self, role_name: str, role_description = 'Allow Redshift clusters to call AWS services on your behalf.') -> None:
    """Create the IAM role

    Args:
        role_name (str): The role name
        role_description (str, optional): The role description. Defaults to 'Allow Redshift clusters to call AWS services on your behalf.'.

    Raises:
        ValueError: If the iam resource is not initialized
    """
    try:
      if self.iam is None:
        raise ValueError("Iam resource not initialized")
      
      role_already_exists = self.get_iam_role(role_name=role_name)
      if role_already_exists:
        print("Role already exists")
        return role_already_exists
      
      print("Creating the role:", role_name)
      self.iam.create_role(
        Path='/',
        RoleName=role_name,
        Description=role_description,
        AssumeRolePolicyDocument=json.dumps(
          {
            'Statement': [
              {
                'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {
                  'Service': 'redshift.amazonaws.com'
                }
              }
            ],
            'Version': '2012-10-17'
          }
        )
      )
      
      print("Attaching the role policy for S3")
      policyStatus = self.iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
      )['ResponseMetadata']['HTTPStatusCode']
      
      if policyStatus != 200:
        self.delete_iam_role(role_name)
        raise ValueError('Error attaching the policy, the creation was rolled back')
    except Exception as e:
      print("Error creating the role: ", e)


def read_config_file(file_name='dwh.cfg') -> dict:
  """Read the configuration file

  Args:
      file_name (str, optional): The file name to read the configuration from. Defaults to 'dwh.cfg'.

  Returns:
      dict: The configuration values
  """
  config = configparser.ConfigParser()
  config.read_file(open(file_name))
  KEY           = config.get('IAM_MANAGEMENT','IAM_KEY')
  SECRET        = config.get('IAM_MANAGEMENT','IAM_SECRET')
  ROLE_NAME     = config.get('IAM_MANAGEMENT','IAM_ROLE_NAME')
  IAM_S3_REGION = config.get('IAM_MANAGEMENT','IAM_S3_REGION')
  
  return {
    'KEY': KEY,
    'SECRET': SECRET,
    'ROLE_NAME': ROLE_NAME,
    'IAM_S3_REGION': IAM_S3_REGION
  }

def main():
  """Allow the user to get, create or delete the IAM role following the requirements for
  the etl.py script
  """
  # Read the config file
  config = read_config_file()
  args = get_arguments()
  
  # Initialize the iam role resource
  iam_role = IamRole(key=config['KEY'], secret=config['SECRET'], region_name=config['IAM_S3_REGION'])
  
  # Uncomment or comment for delete the role
  if args.option == 'delete':
    iam_role.delete_iam_role(role_name=config['ROLE_NAME'])
  
  # Create the role
  if args.option == 'create':
    iam_role.create_iam_role(role_name=config['ROLE_NAME'])
  
  # Default the get method
  iam_role = iam_role.get_iam_role(role_name=config['ROLE_NAME'])
  print(pd.DataFrame(data=[["IamRole", iam_role]], columns=["Key","Value"]))

if __name__ == "__main__":
  main()