import azure.functions as func
import logging
import json
import psycopg2
import os
from psycopg2.extras import RealDictCursor
from datetime import date, datetime
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient 
from decimal import Decimal

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):  # Convert Decimal to float
            return float(obj)
        return super().default(obj)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Database query function processed a request.')

    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
    }

    # Get query parameter safely
    analyst_email = req.params.get('analyst_email')
    allocation = req.params.get('allocation')
    include_future_payments = req.params.get('include_future_payments')

    # Check if the parameter is missing
    if not analyst_email:
        return func.HttpResponse(
            body=json.dumps({'message': 'Bad Request: Missing required query parameter "analyst_email"'}),
            status_code=400,
            headers=headers
        )

    # Check if the parameter is missing
    if not allocation:
        return func.HttpResponse(
            body=json.dumps({'message': 'Bad Request: Missing required query parameter "allocation"'}),
            status_code=400,
            headers=headers
        )

    # Check if the parameter is missing
    if not include_future_payments:
        return func.HttpResponse(
            body=json.dumps({'message': 'Bad Request: Missing required query parameter "include_future_payments"'}),
            status_code=400,
            headers=headers
        )


    try:
        # Retrieve the secret containing the database credentials
        # For Azure, you would use Azure Key Vault to store and retrieve secrets
        credential = DefaultAzureCredential()
        key_vault_url = os.getenv('key_vault_name')
        secret_client = SecretClient(vault_url=key_vault_url, credential=credential)


        # For this example, we'll use environment variables
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

        base_query = """
            SELECT case_id, payment_reference, first_name, last_name, net_redress_value, 
                payment_method, scheduled_payment_date, assignedtoanalyst, 
                payment_completed_by_analyst, payment_completed_by_analyst_date, 
                total_redress, interest, withheld_tax, address_line_1, address_line_2, 
                address_line_3, address_line_4, address_line_5, postcode
            FROM mtl.master_payment_analyst_vw
            WHERE end_ts = '9999-12-31'
        """

        conditions = []
        params = {}

        # Include only past payments if include_future_payments is false
        if include_future_payments != "true":
            conditions.append("scheduled_payment_date <= CURRENT_DATE")

        # Filter by analyst
        if analyst_email == "na":
            if allocation == "unallocated":
                conditions.append("assignedtoanalyst IS NULL")
            elif allocation == "allocated":
                conditions.append("assignedtoanalyst IS NOT NULL AND payment_completed_by_analyst = false")
            elif allocation == "completed":
                conditions.append("payment_completed_by_analyst = true")
        else:
            if allocation in ["allocated", "completed"]:
                conditions.append("assignedtoanalyst = %(analyst_email)s")
                params["analyst_email"] = analyst_email
                conditions.append(f"payment_completed_by_analyst = {'true' if allocation == 'completed' else 'false'}")

        # Append conditions to base query
        if conditions:
            base_query += " AND " + " AND ".join(conditions)

        #base_query += " ORDER BY case_id"

        cursor.execute(base_query, params)

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