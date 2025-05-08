import azure.functions as func
import logging
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient 

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Database query function processed a request.')

    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
    }

    try:
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
        user = req.params.get('user')
        # Establish a connection
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        sql_statement = f"SELECT access_level_id FROM mtl.USER_ACCESS WHERE user_email = %s LIMIT 1"

        cursor.execute(sql_statement, [user])

        # Fetch all results
        results = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        # Convert the results to JSON
        results_json = json.dumps(results, default=str)  # Use default=str to handle datetime serialization

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

 
