import azure.functions as func
import logging
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient 

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Database post-avaiable-hours function processed a request.')

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
    key_vault_url = "https://mtl-backend.vault.azure.net/"
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
                for availability in request_body:
                    reviewer_id = availability['reviewer_id']
                    date = availability['date']
                    available_hours = availability['available_hours']

                    UPDATE_AVAIALABILITY = """
                    UPDATE mtl.file_reviewer_availability
                    SET available_hours = %s
                    WHERE reviewer_id = %s and date = %s
                    """
                    cursor.execute(UPDATE_AVAIALABILITY, (available_hours, reviewer_id, date))
                    conn.commit()

        return func.HttpResponse(
            body=json.dumps({"message": "Update executed successfully."}),
            status_code=200,
            headers={'Content-Type': 'application/json'}
        )
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers={'Content-Type': 'application/json'}
        )
