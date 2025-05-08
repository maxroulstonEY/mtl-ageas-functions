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
                for update_case in request_body:
                    CASE_ID = update_case['case_id']
                    MAILING_CHECK = update_case['mailing_check']
                    userEmail = update_case['userEmail']
                    batch_number = update_case['batch_number']

                    if MAILING_CHECK == 'pass':
                        sql_statement = f"UPDATE mtl.QC_MAILING SET QC_MAILING_READY = TRUE, QC_USER_EMAIL = %s, QC_INSERT_TS = CURRENT_TIMESTAMP WHERE case_id = %s AND mailing_batch_number = %s"
                        cursor.execute(sql_statement, (userEmail, CASE_ID, batch_number,))

                    elif MAILING_CHECK == 'fail':
                        REMOVAL_REASON = update_case['removal_reason']
                        sql_statement = f"UPDATE mtl.QC_MAILING SET QC_REASON_REMOVE_BATCH = %s, QC_MAILING_READY = FALSE, QC_USER_EMAIL = %s, QC_INSERT_TS = CURRENT_TIMESTAMP WHERE case_id = %s AND mailing_batch_number = %s"
                        cursor.execute(sql_statement, (REMOVAL_REASON, userEmail, CASE_ID, batch_number,))

                    elif MAILING_CHECK == 'reset':
                        sql_statement = f"UPDATE mtl.CASE_ALLOCATION SET CASERELEASE_TS = NULL WHERE case_id = %s; UPDATE mtl.QC_MAILING SET CASE_RESET = TRUE WHERE case_id = %s AND mailing_batch_number = %s;"
                        case_tracker_update = f"UPDATE mtl.CASE_TRACKER SET END_TS = CURRENT_TIMESTAMP WHERE end_ts = '9999-12-31 00:00:00' and case_id = %s"
                        case_tracker_insert = f"INSERT INTO mtl.CASE_TRACKER (case_id, state, sub_state, start_ts, end_ts, audit_log update_user) VALUES (%s, 'Review', 'Case Review Unallocated', CURRENT_TIMESTAMP, '9999-12-31 00:00:00', 'FUNCTION: post-mailing-review', %s)"
                        cursor.execute(sql_statement, (CASE_ID, CASE_ID, batch_number,))
                        cursor.execute(case_tracker_update, (CASE_ID,))
                        cursor.execute(case_tracker_insert, (CASE_ID, userEmail,))
                        

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
