import azure.functions as func
import logging
import json
import psycopg2
import os
from psycopg2.extras import RealDictCursor
from datetime import datetime
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
        logging.info('Request body parsed successfully.')
    except ValueError as e:
        logging.error('Failed to parse request body: %s', str(e))
        return func.HttpResponse(
            body=json.dumps({'message': 'Bad Request: Missing JSON body payload'}),
            status_code=400,
            headers=headers
        )

    try:
        # Retrieve the secret containing the database credentials
        credential = DefaultAzureCredential()
        key_vault_url = os.getenv('key_vault_name')
        secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
        logging.info('Key Vault client initialized.')

        # Retrieve database credentials from Azure Key Vault
        db_host = secret_client.get_secret('db-host').value
        db_port = secret_client.get_secret('db-port').value
        db_name = secret_client.get_secret('db-name').value
        db_user = secret_client.get_secret('db-username').value
        db_password = secret_client.get_secret('db-password').value
        logging.info('Database credentials retrieved from Key Vault.')

        # Construct the connection string with the retrieved credentials
        conn_string = f"host='{db_host}' port='{db_port}' dbname='{db_name}' user='{db_user}' password='{db_password}'"
        logging.info('Database connection string constructed.')

        # Get Query Type
        try:
            query_type = req.params.get('query-type')
        except KeyError:
            return {
                'statusCode': 400,
                'headers': headers,
            'body': json.dumps({
                    'message': 'Bad Request: Missing required query parameter "query-type"'
                })
            }

        # Extract data from the request body
        address_data = request_body.get('address')
        deceased_address_data = request_body.get('deceased_address')

        CONTACT_TYPE = address_data.get('contact_type')
        CASE_ID = address_data.get('case_id')
        OUTCOME = address_data.get('outcome')
        ACTUAL_CONTACT_DATE = address_data.get('actual_contact_dtm')
        CALL_SUMMARY = address_data.get('call_summary')

        PROPOSED_TITLE = address_data.get('proposed_title')
        PROPOSED_FORENAME = address_data.get('proposed_forename')
        PROPOSED_MIDDLE_NAME = address_data.get('proposed_middle_name')
        PROPOSED_SURNAME = address_data.get('proposed_surname')

        sc_approval_required = address_data.get('sc_approval_required')
        audit_log = address_data.get('audit_log')
        
        recalc_reason = address_data.get('recalc_reason')
        payment_type = address_data.get('payment_type')
        customer_info_confirmed = address_data.get('customer_info_confirmed')
        change_name_reason = address_data.get('change_name_reason')
        
        try:
            dt = datetime.fromisoformat(ACTUAL_CONTACT_DATE)
        except ValueError:
            # Try to parse the datetime string manually if fromisoformat fails
            dt = datetime.strptime(ACTUAL_CONTACT_DATE, "%Y-%m-%d %H:%M:%S")
        
        ACTUAL_CONTACT_TIME = dt.isoformat() + "Z"
        contact_tracker_sk = int(address_data.get('contact_tracker_sk'))
        update_user = address_data.get('update_user')
        
        address_data.pop('contact_tracker_sk', None)
        address_data.pop('contact_type', None)
        address_data.pop('outcome', None)
        address_data.pop('actual_contact_dtm', None)
        address_data.pop('sc_approval_required', None)
        address_data.pop('call_summary', None)

        address_data.pop('proposed_title', None)
        address_data.pop('proposed_forename', None)
        address_data.pop('proposed_middle_name', None)
        address_data.pop('proposed_surname', None)
    
        address_data.pop('recalc_reason', None)
        address_data.pop('payment_type', None)
        address_data.pop('customer_info_confirmed', None)

        if sc_approval_required == "Yes":
            #exclude_columns = ['title', 'forename', 'middle_name', 'surname']
            exclude_columns = []

            columns = [col for col in address_data.keys() if col not in exclude_columns]
            values = [address_data[col] if address_data.get(col) != "" else None for col in columns]

            if deceased_address_data:
                deceased_columns = [col for col in deceased_address_data.keys() if col not in exclude_columns]
                deceased_values = [deceased_address_data[col] if deceased_address_data.get(col) != "" else None for col in deceased_columns]
        else:
            columns = list(address_data.keys())
            values = [address_data[col] if address_data.get(col) != "" else None for col in columns]

            if deceased_address_data:
                deceased_columns = list(deceased_address_data.keys())
                deceased_values = [deceased_address_data[col] if deceased_address_data.get(col) != "" else None for col in deceased_columns]


        with psycopg2.connect(conn_string) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                logging.info('Database connection established.')

                logging.info(query_type)

                if sc_approval_required == "Yes":
                    # Update the contact tracker
                    contact_tracker_update = """
                    UPDATE mtl.contact_tracker
                    SET proposed_title_change = %s, proposed_forename_change = %s, proposed_middle_name_change = %s, proposed_surname_change = %s, tl_rejection_reason = NULL, recalc_reason = %s, payment_type = %s, customer_info_confirmed = %s
                    WHERE case_id = %s AND contact_tracker_sk = %s AND end_ts = '9999-12-31 00:00:00';
                    """
                    cursor.execute(contact_tracker_update, (PROPOSED_TITLE, PROPOSED_FORENAME, PROPOSED_MIDDLE_NAME, PROPOSED_SURNAME, recalc_reason, payment_type, customer_info_confirmed, CASE_ID, contact_tracker_sk))
                    logging.info('Contact tracker updated.')

                if query_type == "outbound_call":
                    # Update the contact tracker
                    contact_tracker_update = """
                    UPDATE mtl.contact_tracker
                    SET contact_actual_ts = %s::timestamp, outcome = %s, contact_type = %s, call_summary = %s, update_user = %s, sc_approval_required = %s, audit_log = %s, recalc_reason = %s, payment_type = %s, customer_info_confirmed = %s
                    WHERE case_id = %s AND contact_tracker_sk = %s AND end_ts = '9999-12-31 00:00:00';
                    """
                    cursor.execute(contact_tracker_update, (ACTUAL_CONTACT_TIME, OUTCOME, CONTACT_TYPE, CALL_SUMMARY, update_user, sc_approval_required, audit_log, recalc_reason, payment_type, customer_info_confirmed, CASE_ID, contact_tracker_sk))
                    logging.info('Contact tracker updated.')

                elif query_type == "inbound_call":
                    # Update the contact tracker
                    contact_tracker_update = """
                    UPDATE mtl.contact_tracker
                    SET outcome = %s, contact_type = %s, update_user = %s, call_summary = %s, sc_approval_required = %s, audit_log = %s, recalc_reason = %s, payment_type = %s, customer_info_confirmed = %s
                    WHERE case_id = %s AND contact_tracker_sk = %s AND end_ts = '9999-12-31 00:00:00';
                    """
                    cursor.execute(contact_tracker_update, (OUTCOME, CONTACT_TYPE, update_user, CALL_SUMMARY, sc_approval_required, audit_log, recalc_reason, payment_type, customer_info_confirmed, CASE_ID, contact_tracker_sk))
                    logging.info('Contact tracker updated.')    

                    # Insert inbound call into the contact tracker 
                    contact_tracker_insert = """
                    INSERT INTO mtl.contact_tracker (case_id, outcome, contact_type, contact_channel, call_summary, contact_actual_ts, start_ts, end_ts, update_user, audit_log, sc_approval_required, recalc_reason, payment_type, customer_info_confirmed)
                    VALUES(%s, %s, 'Inbound Call', 'Call', %s, %s, NOW(), NOW(), %s, %s, %s, %s, %s, %s);
                    """
                    cursor.execute(contact_tracker_insert, (CASE_ID, OUTCOME, CALL_SUMMARY, ACTUAL_CONTACT_TIME, update_user, audit_log, sc_approval_required, recalc_reason, payment_type, customer_info_confirmed))
                    logging.info('Contact tracker updated.')

                # Update the existing address record
                update_existing = """UPDATE mtl.address SET end_ts = CURRENT_TIMESTAMP WHERE case_id = %s AND end_ts = '9999-12-31 00:00:00'"""
                cursor.execute(update_existing, (CASE_ID,))
                logging.info('Existing address record updated.')

                # Insert the new address record
                insert_new_address = f"INSERT INTO mtl.address ({', '.join(columns)}, start_ts, end_ts) VALUES ({', '.join(['%s'] * len(columns))}, CURRENT_TIMESTAMP, '9999-12-31 00:00:00')"
                cursor.execute(insert_new_address, values)
                logging.info('New address record inserted.')

                if deceased_address_data:
                    # Update the existing deceased address record
                    update_existing_deceased = f"UPDATE mtl.deceased_address SET end_ts = CURRENT_TIMESTAMP WHERE case_id = %s AND end_ts = '9999-12-31 00:00:00'"
                    cursor.execute(update_existing_deceased, (CASE_ID,))
                    logging.info('Existing deceased address record updated.')

                    # Insert the new deceased address record
                    insert_new_deceased_address = f"INSERT INTO mtl.deceased_address ({', '.join(deceased_columns)}, start_ts, end_ts) VALUES ({', '.join(['%s'] * len(deceased_columns))}, CURRENT_TIMESTAMP, '9999-12-31 00:00:00')"
                    cursor.execute(insert_new_deceased_address, deceased_values)
                    logging.info('New deceased address record inserted.')

        # Return a success response
        return func.HttpResponse(
            body=json.dumps({"message": "Update executed for all cases."}),
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
