import boto3
import json
from datetime import datetime
from botocore.exceptions import ClientError

# Initialize boto3 client for ElastiCache
elasticache = boto3.client('elasticache')

def lambda_handler(event, context):
    action = event.get('action')
    cache_cluster_id = event.get('CacheClusterId')
    replication_group_id = event.get('ReplicationGroupId')
    
    if not action:
        return {
            'statusCode': 400,
            'body': 'Action is required in the event payload.'
        }

    if action == 'create_snapshot':
        if not cache_cluster_id:
            return {
                'statusCode': 400,
                'body': 'CacheClusterId is required for creating a snapshot.'
            }
        # Step 1: Generate a dynamic snapshot name with timestamp
        snapshot_name = generate_snapshot_name(cache_cluster_id)
        # Step 2: Create a snapshot for the given cache cluster
        create_snapshot_response = create_snapshot(cache_cluster_id, snapshot_name)
        # Step 3: Return the result of the snapshot creation
        return {
            'statusCode': 200,
            'body': json.dumps(create_snapshot_response, default=json_serializer)
        }
    
    elif action == 'delete_replication_group':
        if not replication_group_id:
            return {
                'statusCode': 400,
                'body': 'ReplicationGroupId is required for deleting a replication group.'
            }
        # Delete the replication group
        delete_response = delete_replication_group(replication_group_id)
        # Return the result of the deletion
        return {
            'statusCode': 200,
            'body': json.dumps(delete_response, default=json_serializer)
        }

    elif action == 'restore_replication_group':
        if not cache_cluster_id or not replication_group_id:
            return {
                'statusCode': 400,
                'body': 'CacheClusterId and ReplicationGroupId are required for restoring a replication group.'
            }
        # Step 1: Retrieve the available snapshot for the cache cluster
        snapshot_name = get_snapshot(cache_cluster_id)
        if snapshot_name:
            # Step 2: Create a new replication group from the snapshot
            restore_response = create_replication_group_from_snapshot(replication_group_id, snapshot_name)
            # Return the result of the restoration
            return {
                'statusCode': 200,
                'body': json.dumps(restore_response, default=json_serializer)
            }
        else:
            return {
                'statusCode': 400,
                'body': f"No snapshots found for cache cluster {cache_cluster_id}"
            }
    
    else:
        return {
            'statusCode': 400,
            'body': 'Invalid action specified.'
        }

def generate_snapshot_name(cache_cluster_id):
    """ Generate a dynamic snapshot name using cache cluster ID and timestamp """
    timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    return f"{cache_cluster_id}-snapshot-{timestamp}"

def create_snapshot(cache_cluster_id, snapshot_name):
    """ Function to create a snapshot of a cache cluster """
    try:
        response = elasticache.create_snapshot(
            CacheClusterId=cache_cluster_id,
            SnapshotName=snapshot_name
        )
        return response
    except ClientError as e:
        print(f"Error creating snapshot: {e}")
        return str(e)

def delete_replication_group(replication_group_id):
    """ Function to delete a replication group """
    try:
        response = elasticache.delete_replication_group(
            ReplicationGroupId=replication_group_id
        )
        return response
    except ClientError as e:
        return str(e)

def get_snapshot(cache_cluster_id):
    """ Retrieve the available snapshot for the cache cluster """
    try:
        snapshots = elasticache.describe_snapshots(
            CacheClusterId=cache_cluster_id
        )
        if snapshots['Snapshots']:
            return snapshots['Snapshots'][0]['SnapshotName']
    except ClientError as e:
        print(f"Error retrieving snapshot: {e}")
    return None

def create_replication_group_from_snapshot(replication_group_id, snapshot_name):
    """ Function to create a replication group from a snapshot """
    try:
        response = elasticache.create_replication_group(
            ReplicationGroupId=replication_group_id,
            ReplicationGroupDescription='Restored from snapshot',
            SnapshotName=snapshot_name,
            CacheNodeType='cache.t4g.small',  # Specify your node type if necessary
            Engine='redis',  # Specify the engine type (Redis)
            Port=6379,  # Example: specify the port
            NodeGroupConfiguration=[
                {
                    'NodeGroupId': '0001',
                    'PrimaryAvailabilityZone': 'ap-south-1a'
                }
            ],
            AutomaticFailoverEnabled=False,  # Enable if needed
            MultiAZEnabled=False  # Enable if needed
        )
        return response
    except ClientError as e:
        print(f"Error creating replication group: {e}")
        return str(e)

def json_serializer(obj):
    """ Helper function to convert non-serializable objects into JSON serializable format. """
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")