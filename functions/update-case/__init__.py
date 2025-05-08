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

    except ValueError as e:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps({
                'message': 'Bad Request: Missing JSON body payload'
            })
        }

    case_complete = request_body['iscomplete']
    access_level = request_body['access_level']
    case_id = request_body['case_id']
    update_user = request_body['update_user']
    del request_body['iscomplete'] #delete from request body array so that they are not POSTed to the database
    del request_body['access_level']
    columns = []
    placeholders = []
    sql_params = {}

    # Iterate over the items in the request_body to construct the SQL statement
    for key, value in request_body.items():
        columns.append(key)
        placeholders.append('%s')  # Use placeholders for the values
        sql_params[key] = value  # Prepare the parameters for the SQL execution

    # Join the columns and placeholders to construct the INSERT statement
    INSERT_NEW_CASE_ROW = f"""UPDATE mtl.INPUT_FILE_REVIEW SET end_ts = current_timestamp WHERE case_id = '{case_id}' and end_ts = '9999-12-31 00:00:00'; 
                                INSERT INTO mtl.INPUT_FILE_REVIEW ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"""

    # UPDATE THE STATUS OF A CASE SO THAT IT ENTERS WIP STATE FOR A FILE REVIEWER
 
     # Case Reviewers / Admin
    if case_complete and (access_level == 6 or access_level == 1 or access_level == 4):
        UPDATE_STATUS_IN_CASE_ALLOC = f"UPDATE mtl.CASE_ALLOCATION SET casestatusanalyst = 'COMPLETED', casestatusqc = 'NEW', fr_complete_date = CASE WHEN fr_complete_date IS NULL THEN CURRENT_DATE ELSE fr_complete_date END, batch_number = to_char(CURRENT_DATE, 'IYYY-IW') WHERE case_id = %s"
        UPDATE_CASE_TRACKER = f"""UPDATE mtl.CASE_TRACKER SET END_TS = CURRENT_TIMESTAMP WHERE CASE_ID = %s AND END_TS = '9999-12-31 00:00:00'; 
                                INSERT INTO mtl.CASE_TRACKER (CASE_ID, STATE, sub_state,  START_TS,  END_TS, audit_log, update_user)
                                VALUES (%s, 'Review', 'Case Review Completed', CURRENT_TIMESTAMP, '9999-12-31 00:00:00', 'function: update-case', %s)"""
    elif not case_complete and (access_level == 6 or access_level == 1 or access_level == 4):
        UPDATE_STATUS_IN_CASE_ALLOC = f"UPDATE mtl.CASE_ALLOCATION SET casestatusanalyst = 'IN_PROGRESS' WHERE case_id = %s"
        UPDATE_CASE_TRACKER = f"""UPDATE mtl.CASE_TRACKER SET  END_TS = CURRENT_TIMESTAMP WHERE CASE_ID = %s AND END_TS = '9999-12-31 00:00:00' AND sub_state != 'Case Review In Progress'; 
                                INSERT INTO mtl.CASE_TRACKER (CASE_ID, STATE, sub_state,  START_TS,  END_TS, audit_log, update_user)
                                SELECT %s, 'Review', 'Case Review In Progress', CURRENT_TIMESTAMP, '9999-12-31 00:00:00', 'function: update-case', %s
                                WHERE NOT EXISTS (SELECT 1 FROM mtl.CASE_TRACKER WHERE CASE_ID = %s AND END_TS = '9999-12-31 00:00:00' AND sub_state = 'Case Review In Progress')"""
     # QC
    elif case_complete and (access_level == 3 or access_level == 2):
        UPDATE_STATUS_IN_CASE_ALLOC = f"UPDATE mtl.CASE_ALLOCATION SET casestatusqc = 'COMPLETED', qc_complete_ts = CASE WHEN qc_complete_ts IS NULL THEN CURRENT_TIMESTAMP ELSE qc_complete_ts END WHERE case_id = %s"
        UPDATE_CASE_TRACKER = f"""UPDATE mtl.CASE_TRACKER SET  END_TS = CURRENT_TIMESTAMP WHERE CASE_ID = %s AND END_TS = '9999-12-31 00:00:00'; 
                                INSERT INTO mtl.CASE_TRACKER (CASE_ID, STATE, sub_state,  START_TS,  END_TS, audit_log, update_user)
                                VALUES (%s, 'Review', 'Case QC Completed', CURRENT_TIMESTAMP, '9999-12-31 00:00:00', 'function: update-case', %s)"""
    elif not case_complete and (access_level == 3 or access_level == 2):
        UPDATE_STATUS_IN_CASE_ALLOC = f"UPDATE mtl.CASE_ALLOCATION SET casestatusqc = 'IN_PROGRESS' WHERE case_id = %s"
        UPDATE_CASE_TRACKER = f"""UPDATE mtl.CASE_TRACKER SET  END_TS = CURRENT_TIMESTAMP WHERE CASE_ID = %s AND END_TS = '9999-12-31 00:00:00' AND sub_state != 'Case QC In Progress'; 
                                INSERT INTO mtl.CASE_TRACKER (CASE_ID, STATE, sub_state,  START_TS,  END_TS, audit_log, update_user)
                                SELECT %s, 'Review', 'Case QC In Progress', CURRENT_TIMESTAMP, '9999-12-31 00:00:00', 'function: update-case', %s
                                WHERE NOT EXISTS (SELECT 1 FROM mtl.CASE_TRACKER WHERE CASE_ID = %s AND END_TS = '9999-12-31 00:00:00' AND sub_state = 'Case QC In Progress')"""
     # QA
    elif case_complete and (access_level == 9 or access_level == 10):
        UPDATE_STATUS_IN_CASE_ALLOC = f"UPDATE mtl.CASE_ALLOCATION SET casestatusqa = 'COMPLETED', qa_complete_ts = CASE WHEN qa_complete_ts IS NULL THEN CURRENT_TIMESTAMP ELSE qa_complete_ts END WHERE case_id = %s"
        UPDATE_CASE_TRACKER = f"""UPDATE mtl.CASE_TRACKER SET  END_TS = CURRENT_TIMESTAMP WHERE CASE_ID = %s AND END_TS = '9999-12-31 00:00:00'; 
                                INSERT INTO mtl.CASE_TRACKER (CASE_ID, STATE, sub_state,  START_TS,  END_TS, audit_log, update_user)
                                VALUES (%s, 'Review', 'Case QA Completed', CURRENT_TIMESTAMP, '9999-12-31 00:00:00', 'function: update-case', %s)"""
    elif not case_complete and (access_level == 9 or access_level == 10):
        UPDATE_STATUS_IN_CASE_ALLOC = f"UPDATE mtl.CASE_ALLOCATION SET casestatusqa = 'IN_PROGRESS' WHERE case_id = %s"
        UPDATE_CASE_TRACKER = f"""UPDATE mtl.CASE_TRACKER SET  END_TS = CURRENT_TIMESTAMP WHERE CASE_ID = %s AND END_TS = '9999-12-31 00:00:00' AND sub_state != 'Case QA In Progress'; 
                                INSERT INTO mtl.CASE_TRACKER (CASE_ID, STATE, sub_state,  START_TS,  END_TS, audit_log, update_user)
                                SELECT %s, 'Review', 'Case QA In Progress', CURRENT_TIMESTAMP, '9999-12-31 00:00:00', 'function: update-case', %s
                                WHERE NOT EXISTS (SELECT 1 FROM mtl.CASE_TRACKER WHERE CASE_ID = %s AND END_TS = '9999-12-31 00:00:00' AND sub_state = 'Case QA In Progress')"""
     # CTC
    elif case_complete and (access_level == 11):
        UPDATE_STATUS_IN_CASE_ALLOC = f"UPDATE mtl.CASE_ALLOCATION SET casestatusctc = 'COMPLETED', ctc_complete_ts = CASE WHEN ctc_complete_ts IS NULL THEN CURRENT_TIMESTAMP ELSE ctc_complete_ts END WHERE case_id = %s"
        UPDATE_CASE_TRACKER = f"""UPDATE mtl.CASE_TRACKER SET  END_TS = CURRENT_TIMESTAMP WHERE CASE_ID = %s AND END_TS = '9999-12-31 00:00:00'; 
                                INSERT INTO mtl.CASE_TRACKER (CASE_ID, STATE, sub_state,  START_TS,  END_TS, audit_log, update_user)
                                VALUES (%s, 'Review', 'Case CTC Completed', CURRENT_TIMESTAMP, '9999-12-31 00:00:00', 'function: update-case', %s)"""
    elif not case_complete and (access_level == 11):
        UPDATE_STATUS_IN_CASE_ALLOC = f"UPDATE mtl.CASE_ALLOCATION SET casestatusctc = 'IN_PROGRESS' WHERE case_id = %s"
        UPDATE_CASE_TRACKER = f"""UPDATE mtl.CASE_TRACKER SET  END_TS = CURRENT_TIMESTAMP WHERE CASE_ID = %s AND END_TS = '9999-12-31 00:00:00' AND sub_state != 'Case CTC In Progress'; 
                                INSERT INTO mtl.CASE_TRACKER (CASE_ID, STATE, sub_state,  START_TS,  END_TS, audit_log, update_user)
                                SELECT %s, 'Review', 'Case CTC In Progress', CURRENT_TIMESTAMP, '9999-12-31 00:00:00', 'function: update-case', %s
                                WHERE NOT EXISTS (SELECT 1 FROM mtl.CASE_TRACKER WHERE CASE_ID = %s AND END_TS = '9999-12-31 00:00:00' AND sub_state = 'Case CTC In Progress')"""
      # Engineer
    elif case_complete and access_level == 8:
        UPDATE_STATUS_IN_CASE_ALLOC = f"UPDATE mtl.CASE_ALLOCATION SET casestatuser = 'COMPLETED', er_complete_ts = CURRENT_TIMESTAMP WHERE case_id = %s AND END_TS = '9999-12-31 00:00:00'"
        UPDATE_CASE_TRACKER = f"""UPDATE mtl.CASE_TRACKER SET  END_TS = CURRENT_TIMESTAMP WHERE CASE_ID = %s AND END_TS = '9999-12-31 00:00:00'; 
                                INSERT INTO mtl.CASE_TRACKER (CASE_ID, STATE, sub_state,  START_TS,  END_TS, audit_log, update_user)
                                VALUES (%s, 'Review', 'Case Review Completed', CURRENT_TIMESTAMP, '9999-12-31 00:00:00', 'function: update-case', %s)"""         
        #UPDATE_CASE_TRACKER = f"""SELECT mtl.CASE_TRACKER WHERE CASE_ID = %s AND CASE_ID = %s and update_user = %s;"""
    elif not case_complete and access_level == 8:
        UPDATE_STATUS_IN_CASE_ALLOC = f"UPDATE mtl.CASE_ALLOCATION SET casestatuser = 'IN_PROGRESS' WHERE case_id = %s"
        UPDATE_CASE_TRACKER = f"""UPDATE mtl.CASE_TRACKER SET  END_TS = CURRENT_TIMESTAMP WHERE CASE_ID = %s AND END_TS = '9999-12-31 00:00:00' AND sub_state != 'Engineer Referral In Progress'; 
                                INSERT INTO mtl.CASE_TRACKER (CASE_ID, STATE, sub_state,  START_TS,  END_TS, audit_log, update_user)
                                SELECT %s, 'Review', 'Engineer Referral In Progress', CURRENT_TIMESTAMP, '9999-12-31 00:00:00', 'function: update-case', %s
                                WHERE NOT EXISTS (SELECT 1 FROM mtl.CASE_TRACKER WHERE CASE_ID = %s AND END_TS = '9999-12-31 00:00:00' AND sub_state = 'Engineer Referral In Progress')"""


    sql_case_timestamp = f"UPDATE mtl.FILE_REVIEW_STATS SET END_TS = CURRENT_TIMESTAMP, ACTIVE = FALSE WHERE END_TS IS NULL AND CASE_ID = %s AND USER_EMAIL = %s"

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
                cursor.execute(INSERT_NEW_CASE_ROW, tuple(sql_params.values()))

                # QC
                if case_complete and (access_level == 3 or access_level == 2):
                    cursor.execute(UPDATE_STATUS_IN_CASE_ALLOC, (case_id,))
                    cursor.execute(UPDATE_CASE_TRACKER, (case_id, case_id, update_user,))
                    cursor.execute(sql_case_timestamp, (case_id, update_user,))        

                # QA
                elif case_complete and (access_level == 9 or access_level == 10):
                    cursor.execute(UPDATE_STATUS_IN_CASE_ALLOC, (case_id,))
                    cursor.execute(UPDATE_CASE_TRACKER, (case_id, case_id, update_user,))
                    cursor.execute(sql_case_timestamp, (case_id, update_user,))

                # CTC
                elif case_complete and (access_level == 11):
                    cursor.execute(UPDATE_STATUS_IN_CASE_ALLOC, (case_id,))
                    cursor.execute(UPDATE_CASE_TRACKER, (case_id, case_id, update_user,))
                    cursor.execute(sql_case_timestamp, (case_id, update_user,))                    

                elif case_complete:
                    cursor.execute(UPDATE_STATUS_IN_CASE_ALLOC, (case_id,))
                    cursor.execute(UPDATE_CASE_TRACKER, (case_id, case_id, update_user,))
                    cursor.execute(sql_case_timestamp, (case_id, update_user,))

                elif not case_complete:
                    cursor.execute(UPDATE_STATUS_IN_CASE_ALLOC, (case_id,))
                    cursor.execute(UPDATE_CASE_TRACKER, (case_id, case_id, update_user, case_id,))


            # Commit is called automatically when the block exits if no exceptions occurred
   
        # Return a success response
        return func.HttpResponse(
            body=json.dumps({"message": "Update executed for all cases."}),
            status_code=200,
            headers=headers   
        )
        
    except Exception as e:
        return func.HttpResponse(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers=headers   
        )
