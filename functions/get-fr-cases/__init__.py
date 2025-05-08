import azure.functions as func
import logging
import json
import psycopg2
from datetime import date, datetime
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from psycopg2.extras import RealDictCursor


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
    except KeyError:
        return {
            'statusCode': 400,
            'headers': headers,
           'body': json.dumps({
                'message': 'Bad Request: Missing required query parameter "query_type"'
            })
        }

    try:
        if query_type == "unallocated":
            sql_statement = "SELECT CASE_ID, POPULATION_COHORT FROM mtl.CASE_ALLOCATION WHERE (LENGTH(assignedtoanalyst) = 0 OR assignedtoanalyst IS NULL) AND END_TS = '9999-12-31 00:00:00' AND casestatusanalyst = 'NEW' ORDER BY start_ts ASC" 
        if query_type == "bulk_unallocated":
            sql_statement = "SELECT POPULATION_COHORT, count(*) AS unallocated_cases FROM mtl.case_allocation WHERE (length(assignedtoanalyst) = 0 OR assignedtoanalyst IS NULL) AND casestatusanalyst = 'NEW' AND END_TS = '9999-12-31 00:00:00' GROUP BY POPULATION_COHORT ORDER BY POPULATION_COHORT" 
        if query_type == "allocated":
            sql_statement = "SELECT CASE_ID, POPULATION_COHORT, CASESTATUSANALYST, FR_COMPLETE_DATE, ENGINEER_REFERRAL, ASSIGNEDTOANALYSTNAME FROM mtl.CASE_ALLOCATION WHERE LENGTH(assignedtoanalyst) > 1 AND END_TS = '9999-12-31 00:00:00' AND (casestatusanalyst = 'NEW' OR casestatusanalyst = 'IN_PROGRESS') ORDER BY start_ts ASC"
        if query_type == "bulk_allocated":
            sql_statement = "SELECT assignedtoanalyst, assignedtoanalystname, POPULATION_COHORT, count(*) AS allocated_cases FROM mtl.CASE_ALLOCATION WHERE (length(assignedtoanalyst) > 1) AND casestatusanalyst = 'NEW' AND END_TS = '9999-12-31 00:00:00' GROUP BY assignedtoanalyst, assignedtoanalystname, POPULATION_COHORT ORDER BY assignedtoanalystname, POPULATION_COHORT"
        if query_type == "completed":
            sql_statement = "SELECT CASE_ID, POPULATION_COHORT, CASESTATUSANALYST, FR_COMPLETE_DATE, ENGINEER_REFERRAL, ASSIGNEDTOANALYSTNAME FROM mtl.CASE_ALLOCATION WHERE LENGTH(assignedtoanalyst) > 1 AND END_TS = '9999-12-31 00:00:00' AND casestatusanalyst = 'COMPLETED' AND caserelease_ts IS NULL ORDER BY fr_complete_date ASC"
        if query_type == "engineer_referral":
            sql_statement = "SELECT * FROM mtl.CASE_ALLOCATION WHERE LENGTH(assignedtoanalyst) > 1 AND END_TS = '9999-12-31 00:00:00' AND engineer_referral IS NOT NULL AND casestatusanalyst <> 'COMPLETED' ORDER BY start_ts ASC"
        if query_type == "released":
            sql_statement = "SELECT * FROM mtl.CASE_ALLOCATION WHERE caserelease_ts IS NOT NULL AND END_TS = '9999-12-31 00:00:00' ORDER BY caserelease_ts ASC"

        # Retrieve the secret containing the database credentials
        credential = DefaultAzureCredential()
        key_vault_url = "https://mtl-backend.vault.azure.net/"
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

        cursor.execute(sql_statement)

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

 
