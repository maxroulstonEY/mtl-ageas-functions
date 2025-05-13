import azure.functions as func
import logging
import json
import psycopg2
import os
from psycopg2.extras import RealDictCursor
from datetime import date, datetime
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Database query function processed a request.')

    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
    }


    try:
        CASE_ID = req.params.get('case_id')
        query_type = req.params.get('query_type')
    except KeyError:
        return {
            'statusCode': 400,
            'headers': headers,
           'body': json.dumps({
                'message': 'Bad Request: Missing required query parameter "case_id" & query_type'
            })
        }
    
    if query_type == "details":
        sql_statement = f"SELECT * FROM mtl.CASE_DETAILS_VW WHERE CASE_ID = %s"
    elif query_type == "history":
        sql_statement = f"SELECT * FROM mtl.CASE_TRACKER WHERE CASE_ID = %s ORDER BY end_ts"
    elif query_type == "contact":
        sql_statement = f"SELECT * FROM mtl.CONTACT_TRACKER WHERE CASE_ID = %s"


    try:
        credential = DefaultAzureCredential()
        key_vault_url = os.getenv('key_vault_name')
        secret_client = SecretClient(vault_url=key_vault_url, credential=credential)


        # For this we'll use environment variables
        db_host = secret_client.get_secret('db-host').value
        db_port = secret_client.get_secret('db-port').value
        db_name = secret_client.get_secret('db-name').value
        db_user = secret_client.get_secret('db-username').value
        db_password = secret_client.get_secret('db-password').value

        # Construct the connection string with the retrieved credentials
        conn_string = f"host='{db_host}' port='{db_port}' dbname='{db_name}' user='{db_user}' password='{db_password}'"

        # Establish a connection
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor(cursor_factory=RealDictCursor)


        cursor.execute(sql_statement, [CASE_ID])

        # Fetch all results
        results = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        # Convert the results to JSON
        results_json = json.dumps(results, cls=CustomJSONEncoder) 

        # Return the records as a JSON response
        return func.HttpResponse(
            body=results_json,
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

 
