import azure.functions as func
import logging
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient 

## test git commit 1

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
    
    case_id = request_body['case_id']
    update_user = request_body['update_user']   

    columns = []
    values = []
    sql_params = {}

    # Iterate over the items in the request_body to construct the SQL statement
    for key, value in request_body.items():
        columns.append(key)
        values.append('%s')  # Use placeholders for the values
        sql_params[key] = value  # Prepare the parameters for the SQL execution


    if request_body['address_type'] == 'Policy Holder':
        table_name = 'mtl.ADDRESS'
    elif request_body['address_type'] == 'Executor':
        table_name = 'mtl.DECEASED_ADDRESS'
    elif request_body['address_type'] == 'Informant':
       table_name = 'mtl.DECEASED_ADDRESS'


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
                ADDRESS_CHECK = f"SELECT * FROM {table_name} WHERE case_id = %s AND END_TS = '9999-12-31 00:00:00'"
                cursor.execute(ADDRESS_CHECK, (case_id,))
                results = cursor.fetchall()

                if len(results) == 0:
                    INSERT_NEW_ADDRESS = f"INSERT INTO {table_name} ({', '.join(columns)},START_TS,END_TS) VALUES ({', '.join(values)},CURRENT_TIMESTAMP,'9999-12-31 00:00:00')"
                    cursor.execute(INSERT_NEW_ADDRESS, (tuple(sql_params.values())))

                elif len(results) > 0:
                    UPDATE_EXISTING = f"UPDATE {table_name} SET END_TS = CURRENT_TIMESTAMP, UPDATE_USER = %s, WHERE case_id = %s AND END_TS = '9999-12-31 00:00:00'"
                    cursor.execute(UPDATE_EXISTING, (update_user, case_id,))

                    INSERT_NEW_ADDRESS = f"INSERT INTO {table_name} ({', '.join(columns)},START_TS, END_TS) VALUES ({', '.join(values)},CURRENT_TIMESTAMP,'9999-12-31 00:00:00')"
                    cursor.execute(INSERT_NEW_ADDRESS, (tuple(sql_params.values())))


            # Commit is called automatically when the block exits if no exceptions occurred
   
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
