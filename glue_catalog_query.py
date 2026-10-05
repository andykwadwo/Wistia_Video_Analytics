import boto3
import time
import pandas as pd


# Add this temporary diagnostic block:
try:
    sts_client = boto3.client('sts')
    identity = sts_client.get_caller_identity()
    print("====================================")
    print(f"CURRENT AWS USER ARN: {identity['Arn']}")
    print(f"AWS ACCOUNT ID:       {identity['Account']}")
    print("====================================")
except Exception as e:
    print(f"Could not check identity: {e}")


# --- Configuration ---
DATABASE = "fact_media_engagement"
QUERY = "SELECT * FROM fact_media_engagement LIMIT 10"
# Ensure dockeruser has permissions to write to this S3 path
S3_OUTPUT_PATH = "s3://test-oseikwadwo/" 
    
def run_athena_query(query, database, s3_output):
    athena_client = boto3.client('athena')
    
    # 1. Start execution
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': s3_output}
    )
    query_id = response['QueryExecutionId']
    print(f"Query started. ID: {query_id}")
    
    # 2. Wait for completion
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
        
    if status != 'SUCCEEDED':
        raise Exception(f"Athena query failed with status: {status}")
        
    # 3. Pull results into memory
    results = athena_client.get_query_results(QueryExecutionId=query_id)
    
    # 4. Parse row items into a Pandas DataFrame
    rows = results['ResultSet']['Rows']
    columns = [col['Name'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    
    # Standard Athena outputs include headers as the first row index; skip index 0 for data rows
    data_rows = []
    for row in rows[1:]:
        data_rows.append([val.get('VarCharValue', None) for val in row['Data']])
        
    return pd.DataFrame(data_rows, columns=columns)

# Run the process
df = run_athena_query(QUERY, DATABASE, S3_OUTPUT_PATH)
print("\n--- Query Successful! Here is the DataFrame ---")
print(df.head())
