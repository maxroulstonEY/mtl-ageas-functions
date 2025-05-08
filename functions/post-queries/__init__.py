import azure.functions as func
import logging
import json
import psycopg2
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
    
    CASE_ID = request_body['case_id']
    CLAIM_REF = request_body['claim_ref']
    
    ACTION_TYPE = request_body['action_type']
    userEmail = request_body['userEmail']
    QUERY_TYPE = request_body['query_type']
    QUERY_DESC = request_body['query_description']
    QUERY_DATE = request_body['query_date']


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
                # Construct the SQL statement using parameters from the request body
                if ACTION_TYPE == 'new': 
                    sql_statement = f"""INSERT INTO mtl.CONTACT_QUERIES (case_id, claim_reference, open_date, open_user, query_status, query_type, query_description, end_ts, update_user, audit_log) 
                                    VALUES (%s, %s, %s, %s, 'OPEN', %s, %s, '9999-12-31', %s, 'New Query')"""
                    cursor.execute(sql_statement, (CASE_ID, CLAIM_REF, QUERY_DATE, userEmail, QUERY_TYPE, QUERY_DESC, userEmail))

                elif ACTION_TYPE == 'update':
                    QUERY_ID = request_body['queryId']  
                    sql_statement = f"""
                                    UPDATE mtl.contact_queries SET end_ts = NOW() WHERE query_id = %s AND end_ts = '9999-12-31';

                                    INSERT INTO mtl.CONTACT_QUERIES (case_id, claim_reference, QUERY_TYPE, QUERY_DESCRIPTION, update_date, update_user, query_id, query_status, end_ts, audit_log) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'OPEN', '9999-12-31', 'Update Query');"""
                    cursor.execute(sql_statement, (QUERY_ID, CASE_ID, CLAIM_REF, QUERY_TYPE, QUERY_DESC, QUERY_DATE, userEmail, QUERY_ID))

                elif ACTION_TYPE == 'close': 
                    QUERY_ID = request_body['queryId']
                    sql_statement = f"""
                                    UPDATE mtl.contact_queries SET end_ts = NOW() WHERE query_id = %s AND end_ts = '9999-12-31';

                                    INSERT INTO mtl.CONTACT_QUERIES (case_id, claim_reference, QUERY_TYPE, QUERY_DESCRIPTION, closed_date, closed_user, query_id, query_status, end_ts, audit_log) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'CLOSED', '9999-12-31', 'Close Query');"""
                    cursor.execute(sql_statement, (QUERY_ID, CASE_ID, CLAIM_REF, QUERY_TYPE, QUERY_DESC, QUERY_DATE, userEmail, QUERY_ID))

                
            
                
            



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
