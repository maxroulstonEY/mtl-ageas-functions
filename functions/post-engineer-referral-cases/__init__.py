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

        ENGINEER_EMAIL = request_body['engineer_email']
        ENGINEER_NAME = request_body['engineer_name']
        ENGINEER_APPROVAL = request_body['engineer_approval']

    except ValueError as e:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps({
                'message': 'Bad Request: Missing JSON body payload'
            })
        }
    
   
    # Retrieve the secret containing the database credentials
    # For Azure, you would use Azure Key Vault to store and retrieve secrets
    credential = DefaultAzureCredential()
    key_vault_url = "https://mtl-backend.vault.azure.net/"
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
                        if ENGINEER_APPROVAL == 'accepted':
                            for update_case in request_body['case_id']:
                                sql_statement_accepted = f"UPDATE mtl.CASE_ALLOCATION SET casestatuser = 'NEW', casestatusqc = NULL, assignedtoer = %s, assignedtoername = %s, engineer_referral = 'Accepted' WHERE case_id = %s"
                                cursor.execute(sql_statement_accepted, (ENGINEER_EMAIL, ENGINEER_NAME, update_case)) 
                                conn.commit()

                            
                        elif ENGINEER_APPROVAL == 'rejected':
                            CASE_ID = request_body['case_id']
                            sql_statement_rejected = f"UPDATE mtl.CASE_ALLOCATION SET casestatusanalyst = 'NEW', casestatusqc = NULL, engineer_referral = 'Rejected' WHERE case_id = %s"
                            cursor.execute(sql_statement_rejected, (CASE_ID,))

   
        # Return a success response
        return func.HttpResponse(
            body=json.dumps({"message": "Update executed successfully."}),
            status_code=200,
            headers=headers   
        )
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        logging.error("Exception type: %s", type(e).__name__)
        logging.error("Exception message: %s", str(e))
        logging.error("Stack trace:", exc_info=True)
        return func.HttpResponse(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers=headers   
        )
