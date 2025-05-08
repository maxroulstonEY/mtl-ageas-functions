import azure.functions as func
import logging
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient 

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Database post-fr-bulk-allocation function processed a request.')

    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
    }

    try:
        # Parse the JSON body from the request
        request_body = req.get_json()

        # Ensure we have all necessary fields in the request body
        required_fields = ['case_id', 'file_name', 'file_description', 'user_name']
        if not all(field in request_body for field in required_fields):
            return func.HttpResponse(
                body=json.dumps({'message': 'Bad Request: Missing required fields'}),
                status_code=400,
                headers=headers
            )

    except ValueError:
        return func.HttpResponse(
            body=json.dumps({'message': 'Bad Request: Invalid JSON'}),
            status_code=400,
            headers=headers
        )
    
    # Retrieve the secret containing the database credentials
    credential = DefaultAzureCredential()
    key_vault_url = "https://mtl-backend.vault.azure.net/"
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

    try:
        db_host = secret_client.get_secret('db-host').value
        db_port = secret_client.get_secret('db-port').value
        db_name = secret_client.get_secret('db-name').value
        db_user = secret_client.get_secret('db-username').value
        db_password = secret_client.get_secret('db-password').value
    except Exception as e:
        return func.HttpResponse(
            body=json.dumps({'error': f'Error retrieving secrets: {str(e)}'}),
            status_code=500,
            headers=headers
        )

    # Construct the connection string with the retrieved credentials
    conn_string = f"host='{db_host}' port='{db_port}' dbname='{db_name}' user='{db_user}' password='{db_password}'"

    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Directly extract values from the single request_body dictionary
                sql_statement = """INSERT INTO mtl.UPLOADED_FILES (case_id, file_name, file_description, upload_user) 
                                   VALUES (%s, %s, %s, %s)"""
                cursor.execute(sql_statement, (
                    request_body['case_id'],
                    request_body['file_name'],
                    request_body['file_description'],
                    request_body['user_name']
                ))

        # Return a success response
        return func.HttpResponse(
            body=json.dumps({"message": "Update executed successfully."}),
            status_code=200,
            headers=headers   
        )
        
    except Exception as e:
        logging.error(f"Error during database operation: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers=headers   
        )
