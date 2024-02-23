import argparse

def get_arguments() -> argparse.Namespace:
  """Get the command-line arguments for AWS resources management

  Returns:
      argparse.Namespace: The parsed arguments
  """
  # Create the instance
  parser = argparse.ArgumentParser(
    description='Process the input'
  )

  # Add arguments
  parser.add_argument(
    'option', 
    type=str, 
    help='option to manage the resource: (create, delete, get)',
    choices=['create', 'delete', 'get']
  )

  # Parse the command-line arguments
  return parser.parse_args()
