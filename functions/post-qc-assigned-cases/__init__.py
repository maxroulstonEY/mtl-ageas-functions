import azure.functions as func
import logging
import json
import psycopg2
import os
from psycopg2.extras import RealDictCursor
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient 

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Database post-fr-bulk-allocation function processed a request.')

    # Check if the request method is POST
    if req.method != 'POST':
        return func.HttpResponse(
            body=json.dumps({"message": "Method not allowed"}),
            status_code=405,
            headers={'Content-Type': 'application/json'}
        )

    try:
        # Parse the JSON body from the request
        request_body = req.get_json()
    except ValueError as e:
        return func.HttpResponse(
            body=json.dumps({'message': 'Bad Request: Missing or invalid JSON body payload'}),
            status_code=400,
            headers={'Content-Type': 'application/json'}
        )

    # Retrieve the secret containing the database credentials
    credential = DefaultAzureCredential()
    key_vault_url = os.getenv('key_vault_name')
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

    db_host = secret_client.get_secret('db-host').value
    db_port = secret_client.get_secret('db-port').value
    db_name = secret_client.get_secret('db-name').value
    db_user = secret_client.get_secret('db-username').value
    db_password = secret_client.get_secret('db-password').value

    conn_string = f"host='{db_host}' port='{db_port}' dbname='{db_name}' user='{db_user}' password='{db_password}'"

    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                for update_case in request_body:
                    qcemail = update_case['qcemail']
                    qcname = update_case['qcname']
                    case_selection_criteria = update_case['case_selection_criteria']
                    case_id = update_case['case_id']
                    email = update_case['email']

                    UPDATE_STATUS_IN_CASE_ALLOC = """
                        UPDATE mtl.CASE_ALLOCATION
                        SET assignedtoqc = %s, assignedtoqcname = %s, case_selection_criteria = %s
                        WHERE case_id = %s
                        """

                    UPDATE_CASE_TRACKER = """                        
                        INSERT INTO mtl.case_tracker (case_id, state, sub_state, audit_log, update_user, end_ts)
                        SELECT case_id, 'Review', 'QC Allocated', 'FUNCTION: post-qc-assigned-cases', %s, '9999-12-31 00:00:00' 
                        FROM mtl.case_tracker
                        WHERE case_id = %s AND end_ts = '9999-12-31 00:00:00' AND sub_state = 'Case Review Completed';

                        UPDATE mtl.case_tracker SET end_ts = CURRENT_TIMESTAMP 
                        WHERE case_id = %s AND end_ts = '9999-12-31 00:00:00' AND sub_state = 'Case Review Completed';
                        """

                    cursor.execute(UPDATE_STATUS_IN_CASE_ALLOC, (qcemail, qcname, case_selection_criteria, case_id,))
                    cursor.execute(UPDATE_CASE_TRACKER, (email, case_id, case_id,))
                    conn.commit()

        return func.HttpResponse(
            body=json.dumps({"message": "Update executed successfully."}),
            status_code=200,
            headers={'Content-Type': 'application/json'}
        )
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({"error": "An error occurred while updating the database."}),
            status_code=500,
            headers={'Content-Type': 'application/json'}
        )
