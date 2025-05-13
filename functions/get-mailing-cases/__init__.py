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
        query_type = req.params.get('query_type')
        batch_number = req.params.get('batch_number')
    except KeyError:
        return {
            'statusCode': 400,
            'headers': headers,
           'body': json.dumps({
                'message': 'Bad Request: Missing required query parameter(s): query_type / batch_number'
            })
        }

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

        if query_type == "cut_batch":
            sql_statement = "SELECT * FROM mtl.QC_MAILING_VW"
            cursor.execute(sql_statement)
        elif query_type == "qc_review":
            sql_statement = "SELECT * FROM mtl.QC_MAILING_STATS_VW"
            cursor.execute(sql_statement)
        elif query_type == "qc_batch_review":
            sql_statement = f"SELECT * FROM mtl.QC_MAILING_SCREEN_VW WHERE MAILING_BATCH_NUMBER = %s"
            cursor.execute(sql_statement, (batch_number))
        elif query_type == "mailing":
            sql_statement = "SELECT * FROM mtl.QC_MAILING WHERE QC_MAILING_READY = TRUE"
            cursor.execute(sql_statement)
        elif query_type == "mailing_removal":
            sql_statement = "SELECT * FROM mtl.METADATA_MAILING_REMOVAL WHERE ACTIVE = TRUE"
            cursor.execute(sql_statement)

        # Fetch all results
        results = cursor.fetchall()


        # Close the cursor and connection
        cursor.close()
        conn.close()

        # Convert the results to JSON
        results_json = json.dumps(results, cls=CustomJSONEncoder)  # Use default=str to handle datetime serialization

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

 
