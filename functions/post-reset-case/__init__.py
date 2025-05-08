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
    RESET_TYPE = request_body['reset_type']
    EMAIL = request_body['userEmail']

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
                if RESET_TYPE == 'fr':
                    REASON = ''
                    sql_statement = f"UPDATE mtl.CASE_ALLOCATION SET casestatusanalyst = 'IN_PROGRESS', casestatusqc = NULL, assignedtoqc = NULL, assignedtoqcname = NULL, fr_complete_date = NULL, batch_number = NULL, case_selection_criteria = NULL, engineer_referral = NULL WHERE case_id = %s"
                    sql_logging_statement = f"INSERT INTO mtl.LOG_TABLE_BUTTONS (CASE_ID, BUTTON_CLICKED, USER_EMAIL, INSERT_TS, REASON) VALUES (%s, 'Return to FR', %s, CURRENT_TIMESTAMP, %s)"
                if RESET_TYPE == 'qc':
                    REASON = ''
                    sql_statement = f"UPDATE mtl.CASE_ALLOCATION SET casestatusqc = 'IN_PROGRESS', qc_complete_ts = NULL WHERE case_id = %s"
                    sql_logging_statement = f"INSERT INTO mtl.LOG_TABLE_BUTTONS (CASE_ID, BUTTON_CLICKED, USER_EMAIL, INSERT_TS, REASON) VALUES (%s, 'Return to QC', %s, CURRENT_TIMESTAMP, %s)"
                if RESET_TYPE == 'descope':
                    REASON = request_body['descope_reason']
                    sql_statement = f"UPDATE mtl.CASE_ALLOCATION SET end_TS = current_timestamp WHERE case_id = %s"
                    sql_logging_statement = f"INSERT INTO mtl.LOG_TABLE_BUTTONS (CASE_ID, BUTTON_CLICKED, USER_EMAIL, INSERT_TS, REASON) VALUES (%s, 'Descope', %s, CURRENT_TIMESTAMP, %s)"
                    sql_tracker_update = f"UPDATE mtl.CASE_TRACKER SET end_ts = CURRENT_TIMESTAMP WHERE case_id = %s and end_ts = '9999-12-31 00:00:00'"
                    sql_tracker_insert = f"INSERT INTO mtl.CASE_TRACKER (case_id, state, sub_state, start_ts, end_ts, audit_log, update_user, descope_reason) VALUES (%s, 'Closed', 'Descoped', CURRENT_TIMESTAMP, '9999-12-31 00:00:00', 'FUNCTION: post-reset-case', , %s, %s)"
                if RESET_TYPE == 'reset':
                    REASON = request_body['reset_reason']
                    sql_statement = f"UPDATE mtl.CASE_ALLOCATION SET CASERELEASE_TS = NULL WHERE case_id = %s;"
                    sql_logging_statement = f"INSERT INTO mtl.LOG_TABLE_BUTTONS (CASE_ID, BUTTON_CLICKED, USER_EMAIL, INSERT_TS, REASON) VALUES (%s, 'FR Reset', %s, CURRENT_TIMESTAMP, %s)"
                    sql_tracker_update = f"UPDATE mtl.CASE_TRACKER SET end_ts = CURRENT_TIMESTAMP WHERE case_id = %s and end_ts = '9999-12-31 00:00:00'"
                    sql_tracker_insert = f"INSERT INTO mtl.CASE_TRACKER (case_id, state, sub_state, start_ts, end_ts, audit_log, update_user) VALUES (%s, 'Review', 'Case Review Unallocated', CURRENT_TIMESTAMP, '9999-12-31 00:00:00', 'FUNCTION: post-reset-case',  %s)"
                
                if RESET_TYPE == 'constrain':
                    REASON = ''
                    CONSTRAINT = request_body['constrain_reason']
                    sql_statement = f" SELECT %s;"
                    sql_logging_statement = f"INSERT INTO mtl.LOG_TABLE_BUTTONS (CASE_ID, BUTTON_CLICKED, USER_EMAIL, INSERT_TS, REASON) VALUES (%s, 'Constrain', %s, CURRENT_TIMESTAMP, %s)"
                    sql_tracker_insert = f"INSERT INTO mtl.metadata_constraints_summary (case_id, constraint_code, constraint_desc, start_ts, end_ts, insert_ts)  VALUES (%s, 'CC555', 'Frontend: {CONSTRAINT}', CURRENT_TIMESTAMP, '9999-12-31 00:00:00', CURRENT_TIMESTAMP)"
                if RESET_TYPE == 'unconstrain':
                    REASON = ''
                    sql_statement = f" SELECT %s;"
                    sql_logging_statement = f"INSERT INTO mtl.LOG_TABLE_BUTTONS (CASE_ID, BUTTON_CLICKED, USER_EMAIL, INSERT_TS, REASON) VALUES (%s, 'Unconstrain', %s, CURRENT_TIMESTAMP, %s)"
                    sql_tracker_update = f"UPDATE mtl.metadata_constraints_summary SET end_ts = CURRENT_TIMESTAMP WHERE CASE_ID = %s AND constraint_code = 'CC555';"

                dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE']

                if not any(keyword in sql_statement.upper() for keyword in dangerous_keywords):
                    cursor.execute(sql_statement, (CASE_ID,))
                    cursor.execute(sql_logging_statement, (CASE_ID, EMAIL, REASON,))
                    if RESET_TYPE == 'descope':
                        cursor.execute(sql_tracker_update, (CASE_ID,))
                        cursor.execute(sql_tracker_insert, (CASE_ID, EMAIL, REASON,))
                    if RESET_TYPE == 'reset':
                        cursor.execute(sql_tracker_update, (CASE_ID,))
                        cursor.execute(sql_tracker_insert, (CASE_ID, EMAIL,))
                    if RESET_TYPE == 'constrain':
                        cursor.execute(sql_tracker_insert, (CASE_ID,))
                    if RESET_TYPE == 'unconstrain':
                        cursor.execute(sql_tracker_update, (CASE_ID,))
                else:
                    raise Exception('Detected dangerous keyword.') 


        # Return a success response
        return func.HttpResponse(
            body=json.dumps({"Update executed for all cases."}),
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
