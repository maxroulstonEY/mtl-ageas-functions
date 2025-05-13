import azure.functions as func
import logging
import json
import psycopg2
import os
from psycopg2.extras import RealDictCursor
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient 

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Database insert function processed a request.')

    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
    }
    
    try:
        # Parse the JSON body from the request
        request_body = req.get_json()

    except KeyError:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps({
                'message': 'Bad Request: Missing JSON body payload'
            })
        }
    
    case_id = request_body['case_id']
    user_Email = request_body['userEmail']
    role = request_body['role']


        # Retrieve the secret containing the database credentials
    # For Azure, you would use Azure Key Vault to store and retrieve secrets
    credential = DefaultAzureCredential()
    key_vault_url = os.getenv('key_vault_name')
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)


    # For this example, we'll use environment variables
    db_host = secret_client.get_secret('db-host').value
    db_port = secret_client.get_secret('db-port').value
    db_name = secret_client.get_secret('db-name').value
    db_user = secret_client.get_secret('db-username').value
    db_password = secret_client.get_secret('db-password').value

    # Construct the connection string with the retrieved credentials
    conn_string = f"host='{db_host}' port='{db_port}' dbname='{db_name}' user='{db_user}' password='{db_password}'"


    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Construct the SQL statement using parameters from the request body
                case_check_sql = f"SELECT * FROM mtl.FILE_REVIEW_STATS WHERE case_id = %s AND active = true"
                cursor.execute(case_check_sql, (case_id,))
                results = cursor.fetchall()

                if len(results) == 0:
                    sql_statement = f"""INSERT INTO mtl.FILE_REVIEW_STATS (case_id, user_email, start_ts, role, active) 
                                    VALUES (%s, %s, CURRENT_TIMESTAMP, %s, true)"""
                    cursor.execute(sql_statement, (case_id, user_Email, role,))


        # Return a success response
        return func.HttpResponse(
            body=json.dumps({"message": "Update executed for all cases."}),
            status_code=200,    
            headers={'Content-Type': 'application/json'}   
        )
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        logging.error("Exception type: %s", type(e).__name__)
        logging.error("Exception message: %s", str(e))
        logging.error("Stack trace:", exc_info=True)
        return func.HttpResponse(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers={'Content-Type': 'application/json'}      
        )
