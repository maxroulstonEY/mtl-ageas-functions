import azure.functions as func
import logging
import json
import psycopg2
import os
from psycopg2.extras import RealDictCursor
from datetime import date, datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient 

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return str(obj)  # Or obj.total_seconds() if you prefer seconds
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

    # if query_type == "fr":
    #     sql_statement = "SELECT * FROM PUBLIC.DASHBOARD_FR_VW"
    # elif query_type == "qc":
    #     sql_statement = "SELECT * FROM PUBLIC.DASHBOARD_QC_VW"
    # elif query_type == "rm":
    #     sql_statement = "SELECT * FROM PUBLIC.DASHBOARD_RM_VW"
    if query_type == "case_tracker":
        sql_statement = """WITH last_friday AS (
                            SELECT date_trunc('week', CURRENT_DATE) - interval '2 days' + interval '1 second' AS end_of_last_friday),
                        last_week_states AS (
                            SELECT
                                ct.state, ct.sub_state, pm.cohort, COUNT(*) AS last_week_count
                            FROM mtl.CASE_TRACKER ct
                            LEFT JOIN mtl.population_master pm ON ct.case_id = pm.case_id
                            WHERE
                                ct.start_ts <= (SELECT end_of_last_friday FROM last_friday)
                                AND (ct.end_ts > (SELECT end_of_last_friday FROM last_friday) OR ct.end_ts = '9999-12-31 00:00:00')
                            GROUP BY ct.state, ct.sub_state, pm.cohort
                        ),
                        current_live_states AS (
                            SELECT
                                ct.state, ct.sub_state, pm.cohort, COUNT(*) AS current_live_count
                            FROM 
                                mtl.CASE_TRACKER ct
                            LEFT JOIN
                                mtl.CASE_ALLOCATION ca ON ct.case_id = ca.case_id
                            LEFT JOIN
                                mtl.POPULATION_MASTER pm ON ct.case_id = pm.case_id
                            WHERE
                                ct.end_ts = '9999-12-31 00:00:00'
                            GROUP BY
                                ct.state, ct.sub_state, pm.cohort
                        )
                        SELECT
                            c.state, c.sub_state, c.cohort, COALESCE(l.last_week_count, 0) AS last_week_count, COALESCE(c.current_live_count, 0) AS current_live_count
                        FROM
                            current_live_states c
                        LEFT JOIN
                            last_week_states l
                        ON
                            c.state = l.state
                            AND c.sub_state = l.sub_state
                            AND c.cohort = l.cohort
                        UNION ALL
                        SELECT
                            l.state, l.sub_state, l.cohort, COALESCE(l.last_week_count, 0) AS last_week_count, 0 AS current_live_count
                        FROM 
                            last_week_states l
                        LEFT JOIN 
                            current_live_states c
                        ON
                            l.state = c.state
                            AND l.sub_state = c.sub_state
                            AND l.cohort = c.cohort
                        WHERE
                            c.state IS NULL
                        ORDER BY
                            state, sub_state, cohort
                        """
    elif query_type == "file_review":
        sql_statement = "SELECT * FROM mtl.file_review_stats_vw"
    elif query_type == "quality":
        sql_statement = """SELECT ifr.qc_review_outcome, assignedtoanalyst, assignedtoanalystname, reporting_manager, COUNT(*) 
                            FROM mtl.case_allocation ca
                            LEFT JOIN mtl.user_access ua on ua.user_email = ca.assignedtoanalyst
                            LEFT JOIN ( SELECT DISTINCT max(input_file_review.input_file_review_sk) AS id_sk,
                                    input_file_review.case_id
                                FROM mtl.input_file_review
                                GROUP BY input_file_review.case_id) z ON ca.case_id::text = z.case_id::text
                            LEFT JOIN mtl.input_file_review ifr ON z.id_sk = ifr.input_file_review_sk AND z.case_id::text = ifr.case_id::text
                            WHERE ifr.qc_review_outcome <> '' group by 1, 2, 3, 4
                        UNION 
                            SELECT ifr.qc_review_outcome, 'All Users', 'All Users', reporting_manager, COUNT(*) 
                                FROM mtl.case_allocation ca
                                LEFT JOIN mtl.user_access ua on ua.user_email = ca.assignedtoanalyst
                                LEFT JOIN ( SELECT DISTINCT max(input_file_review.input_file_review_sk) AS id_sk,
                                        input_file_review.case_id
                                    FROM mtl.input_file_review
                                    GROUP BY input_file_review.case_id) z ON ca.case_id::text = z.case_id::text
                                LEFT JOIN mtl.input_file_review ifr ON z.id_sk = ifr.input_file_review_sk AND z.case_id::text = ifr.case_id::text
                                WHERE ifr.qc_review_outcome <> '' group by ifr.qc_review_outcome, reporting_manager
                        UNION 
                            SELECT ifr.qc_review_outcome, 'All Users', 'All Users', 'All', COUNT(*) 
                                FROM mtl.case_allocation ca
                                LEFT JOIN ( SELECT DISTINCT max(input_file_review.input_file_review_sk) AS id_sk,
                                        input_file_review.case_id
                                    FROM mtl.input_file_review
                                    GROUP BY input_file_review.case_id) z ON ca.case_id::text = z.case_id::text
                                LEFT JOIN mtl.input_file_review ifr ON z.id_sk = ifr.input_file_review_sk AND z.case_id::text = ifr.case_id::text
                                WHERE ifr.qc_review_outcome <> '' group by 1"""


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

 
