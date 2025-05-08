import azure.functions as func
import logging
import json
from openpyxl import Workbook
import io
from datetime import datetime
from psycopg2.extras import RealDictCursor
import psycopg2
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'OPTIONS,GET'
}

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Database query function processed a request.')

    try:
        OBJECT_NAME = req.params.get('object_name')
        TAB_NAME = req.params.get('tab_name')
        if not OBJECT_NAME or not TAB_NAME:
            raise KeyError
    except KeyError:
        return func.HttpResponse(
            body=json.dumps({'message': 'Bad Request: Missing required query parameter(s)'}),
            status_code=400,
            headers=headers
        )

    try:
        credential = DefaultAzureCredential()
        key_vault_url = "https://mtl-backend.vault.azure.net/"
        secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

        # Retrieve the secret containing the database credentials
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

        # Execute a SELECT query for metadata table to return row headers for excel and sql query for mi report
        query = f"SELECT * FROM MTL.MI_METADATA_EXPORT WHERE object_name = '{OBJECT_NAME}' AND tab_name = '{TAB_NAME}' AND object_active = true"
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'UPDATE', 'ALTER', 'CREATE', 'GRANT']

        if not any(keyword in query.upper() for keyword in dangerous_keywords):
            cursor.execute(query)
        else:
            raise Exception('Detected dangerous keyword.')

        # Fetch all results
        results = cursor.fetchall()

        # Set query for mi, execute and return results
        mi_sql_query = results[0]["sql"]
        mi_file_name = results[0]["mi_file_name"]
        cursor.execute(mi_sql_query)
        rows = cursor.fetchall()

        # Combine column names and rows
        result = []
        col_names = list(rows[0].keys())
        result.append(col_names)  # Add column names as the first element in the result list
        # Extract values from each JSON object and add them to the result list
        for obj in rows:
            row_values = [obj[col] for col in col_names]  # Extract values corresponding to each column
            result.append(row_values)

        # Close the cursor and connection
        cursor.close()
        conn.close()

        # Create a new workbook
        wb = Workbook()

        # Access the active sheet
        ws = wb.active

        # Name the sheet
        ws.title = TAB_NAME

        # Add the rows to the sheet
        for row in result:
            ws.append(row)

        # Save the workbook to a buffer
        excel_buffer = io.BytesIO()
        wb.save(excel_buffer)
        excel_buffer.seek(0)

        # Get the current date and time
        current_datetime = datetime.now()
        formatted_datetime = current_datetime.strftime('%Y%m%d_%H%M%S')
        new_file_key = f'{mi_file_name}_{formatted_datetime}.xlsx'

        # Upload the new Excel file to Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(secret_client.get_secret('storage-connection-string').value)
        blob_client = blob_service_client.get_blob_client(container='mi-exports', blob=new_file_key)
        blob_client.upload_blob(excel_buffer, overwrite=True)

        return func.HttpResponse(
            body=json.dumps({'message': 'MI Report Created'}),
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
