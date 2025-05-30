from fastapi import APIRouter
from fastapi import FastAPI, Query, HTTPException
from langchain_openai import ChatOpenAI
from pydantic import BaseModel
import logging
import os
from langchain_core.prompts import ChatPromptTemplate
import duckdb

router = APIRouter()

# OpenAI Model for SQL generation and summarization
# OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # production
OPENAI_API_KEY = ""  # dev
logging.info({OPENAI_API_KEY})
llm = ChatOpenAI(model="gpt-4o-mini", api_key=OPENAI_API_KEY)
# SQL Prompt Template
sql_prompt = ChatPromptTemplate.from_template(
    "Convert this natural language question into a SQL query: {query}"
)

read_paths_dev = {
    "datasnake_sensor_data_processed_deltalake": "C:\\datasnake\\prab\\dev\\datasnake-sensor-data\\deltalake-sensor-data-processed",
}
read_paths = {
    "datasnake_sensor_data_processed_deltalake": "/home/resources/deltalake-sensor-data-processed",
}


# Request schema
class QueryRequest(BaseModel):
    query: str


# @router.get("/query-full-dataset")
# async def query_llm():
#     print("inside query_llm")
#     return {"query": "OPENAIKEY", "message": "Item retrieved"}


@router.get("/query-full-dataset")
async def query_llm():
    try:
        print("inside query_llm openai")
        # What is the average temperature recorded for February 21 2025?
        request = QueryRequest(
            query="what is the max temperature recorded for February 21 2025?"
        )
        # print(request)

        actual_schema = fetch_table_schema()
        # Generate SQL query using OpenAI LLM
        sql_prompt = ChatPromptTemplate.from_template(
            """
            You are an expert DUCKDB SQL assistant. Convert the following natural language question into a SQL query.
            Use the table name `sensor_data` in your query. This will be replaced later programmatically.
            Use DuckDB-compatible syntax only. Avoid PostgreSQL-specific functions like `TO_CHAR`.

            Here are some working examples of SQL queries that work correctly with this schema for your reference
            to take to build the sql query:

            -- Average temperature on a single date
            SELECT 
                AVG(temp) AS avg_temp 
            FROM sensor_data 
            WHERE CAST(timestamp AS DATE) = DATE '2025-02-21';

            -- Hourly average temperature on a specific day
            SELECT 
                hour(timestamp) AS hour_start, 
                AVG(temp) AS avg_temp 
            FROM sensor_data 
            WHERE CAST(timestamp AS DATE) = DATE '2025-02-21' 
            GROUP BY hour_start 
            ORDER BY hour_start;

            -- Average temperature over a date range
            SELECT 
                CAST(timestamp AS DATE) AS day, 
                AVG(temp) AS avg_temp 
            FROM sensor_data 
            WHERE timestamp BETWEEN TIMESTAMP '2025-02-21 00:00:00' AND TIMESTAMP '2025-03-01 00:00:00' 
            GROUP BY day 
            ORDER BY day;

            -- Weekly average temperature
            SELECT 
                DATE_TRUNC('week', timestamp) AS week_start, 
                AVG(temp) AS avg_temp 
            FROM sensor_data 
            GROUP BY week_start 
            ORDER BY week_start;

            -- Monthly average temperature
            SELECT 
                DATE_TRUNC('month', timestamp) AS month_start, 
                AVG(temp) AS avg_temp 
            FROM sensor_data 
            GROUP BY month_start 
            ORDER BY month_start;
            
            **Schema**:
            {schema}
            
            Ensure the SQL works correctly if the `timestamp` field is in datetime format (e.g. 'YYYY-MM-DD HH:MM:SS').

            Note: `timestamp` is stored in full datetime format (e.g., '2025-03-25 14:30:00').
            
            Only return the SQL query — no explanations, no markdown, and no code block formatting.

            **Natural language query:** {query}
            
            SQL:
            """
        )

        sql_chain = sql_prompt | llm
        sql_query = (
            sql_chain.invoke({"query": request.query, "schema": actual_schema})
            .content.strip()
            .rstrip(";")
        )
        print("print the sql_query:", sql_query)

        corrected_query = sql_query.replace(
            "FROM sensor_data",
            f"FROM delta_scan('{read_paths_dev['datasnake_sensor_data_processed_deltalake']}')",
        )

        print(f"Generating Corrected SQL: {corrected_query}")
        # above corrected_query is not being used here though
        # check /query-prompt-dataset for it to be in action

        # Execute SQL query
        con = duckdb.connect()
        duckdb.sql(
            """
            INSTALL delta;
            LOAD delta;
            """
        )
        # query = f"""
        # SELECT *
        # FROM delta_scan('{read_paths_dev['datasnake_sensor_data_processed_deltalake']}')
        # WHERE postal_code != 000000
        # limit 10
        # """

        debug_query = f"""
        SELECT temp, timestamp, CAST(timestamp as DATE) AS formatted_timestamp 
        FROM delta_scan('C:\\datasnake\\prab\\dev\\datasnake-sensor-data\\deltalake-sensor-data-processed')
        WHERE formatted_timestamp = '2025-02-21'
        limit 10
        """
        debug_query = f"""
        SELECT 
        CAST(timestamp AS DATE) AS date,
        AVG(temp) AS avg_temp
        FROM delta_scan('C:\\datasnake\\prab\\dev\\datasnake-sensor-data\\deltalake-sensor-data-processed')
        WHERE postal_code != 000000 
        AND CAST(timestamp AS DATE) = DATE '2025-02-21'
        GROUP BY date
        """
        debug_query_incorrect_hours = f"""
        SELECT 
        hour(CAST(timestamp AS TIMESTAMP)) AS hour_start,
        (temp) AS avg_temp
        FROM delta_scan('C:\\datasnake\\prab\\dev\\datasnake-sensor-data\\deltalake-sensor-data-processed')
        WHERE postal_code != 000000 
        AND CAST(timestamp AS DATE) = DATE '2025-02-21'
        -- GROUP BY hour_start
        ORDER BY hour_start
        limit 50
        """
        debug_query1 = f"""
        SELECT 
            CAST(timestamp AS DATE) AS date,
            AVG(temp) AS avg_temp
        FROM delta_scan('C:\\datasnake\\prab\\dev\\datasnake-sensor-data\\deltalake-sensor-data-processed')
        WHERE postal_code != 000000 
        AND CAST(timestamp AS DATE) BETWEEN DATE '2025-02-20' AND DATE '2025-02-25'
        GROUP BY date
        ORDER BY date
        """
        debug_query1 = f"""
        SELECT 
            DATE_TRUNC('hour', timestamp) AS hour_start,
            AVG(temp) AS avg_temp
        FROM delta_scan('C:\\datasnake\\prab\\dev\\datasnake-sensor-data\\deltalake-sensor-data-processed')
        WHERE postal_code != 000000 
        AND CAST(timestamp AS DATE) BETWEEN DATE '2025-02-20' AND DATE '2025-02-25'
        GROUP BY hour_start
        ORDER BY hour_start
        """
        debug_query1 = f"""
        SELECT 
            DATE_TRUNC('week', timestamp) AS week_start,
            AVG(temp) AS avg_temp
        FROM delta_scan('C:\\datasnake\\prab\\dev\\datasnake-sensor-data\\deltalake-sensor-data-processed')
        WHERE postal_code != 000000 
        GROUP BY week_start
        ORDER BY week_start
        """
        debug_query1 = f"""
        SELECT 
            DATE_TRUNC('month', timestamp) AS month_start,
            AVG(temp) AS avg_temp
        FROM delta_scan('C:\\datasnake\\prab\\dev\\datasnake-sensor-data\\deltalake-sensor-data-processed')
        WHERE postal_code != 000000 
        GROUP BY month_start
        ORDER BY month_start
        """
        debug_query1 = f"""
        SELECT 
            DATE_TRUNC('year', timestamp) AS year_start,
            AVG(temp) AS avg_temp
        FROM delta_scan('C:\\datasnake\\prab\\dev\\datasnake-sensor-data\\deltalake-sensor-data-processed')
        WHERE postal_code != 000000 
        GROUP BY year_start
        ORDER BY year_start
        """
        debug_query = f"""
        SELECT DISTINCT CAST(timestamp as DATE) AS dates
        FROM delta_scan('C:\\datasnake\\prab\\dev\\datasnake-sensor-data\\deltalake-sensor-data-processed')
        WHERE postal_code != 000000 
        """
        print("debugging query:", debug_query)
        result_df = con.query(corrected_query).pl()
        con.close()
        print(result_df.head())
        # columns = [desc[0] for desc in con.description]
        # print()
        # result_data = [dict(zip(columns, row)) for row in result]
        result_data = result_df.to_dicts()
        # print(result_data)
        # Summarize results
        summary_prompt = f"Analyze this data and summarize key insights: {result_data}"
        summary = llm.invoke(summary_prompt)

        return {
            "query": request.query,
            "sql": corrected_query,
            "data": result_data,
            "summary": summary,
        }
    except Exception as e:
        print("caught inside query_llm: ", str(e))
        raise HTTPException(status_code=500, detail=str(e))


def fetch_table_schema():
    """Retrieve the table schema dynamically from DeltaLake via DuckDB."""
    con = duckdb.connect()
    duckdb.sql("INSTALL delta; LOAD delta;")  # Load Delta extension
    DELTA_TABLE_PATH = "datasnake_sensor_data_processed_deltalake"
    # Fetch schema
    schema_query = f"DESCRIBE SELECT * FROM delta_scan('{read_paths_dev['datasnake_sensor_data_processed_deltalake']}') LIMIT 1;"
    schema_df = con.query(schema_query).pl()
    con.close()

    # Extract column names and types
    schema = [
        f"{row['column_name']} ({row['column_type']})" for row in schema_df.to_dicts()
    ]

    return "\n".join(schema)


@router.get("/query-prompt-dataset")
# async def query_llm(request: QueryRequest):
async def summarize_query_llm():
    try:
        print("inside summarize_query_llm openai")
        request = QueryRequest(query="What is the average temperature recorded?")
        # print(request)

        # Get actual table schema from DeltaLake
        actual_schema = fetch_table_schema()
        print(f"🟢 Retrieved Schema:\n{actual_schema}")

        # Generate SQL query from OpenAI LLM natural language
        sql_prompt = ChatPromptTemplate.from_template(
            f"""
        You are an expert SQL assistant. Convert the following natural language question into a SQL query.
        
        **Table Name**: sensor_data  
        **Schema**:  
        {actual_schema}
        
        Ensure the SQL correctly references the column names as per the schema above.  
        **Natural language query:** {{query}}
        """
        )

        sql_chain = sql_prompt | llm
        sql_query = sql_chain.invoke({"query": request.query}).content
        corrected_query = sql_query.replace(
            " FROM sensor_data",
            f" FROM delta_scan('{read_paths_dev['datasnake_sensor_data_processed_deltalake']}')",
        )
        print(f"Generated Corrected SQL: {corrected_query}")

        # Execute SQL query
        con = duckdb.connect()
        duckdb.sql(
            """
            INSTALL delta;
            LOAD delta;
            """
        )

        result_df = con.query(corrected_query).pl()
        con.close()
        print(result_df.head())
        # columns = [desc[0] for desc in con.description]
        # print()
        # result_data = [dict(zip(columns, row)) for row in result]
        result_data = result_df.to_dicts()
        print(result_data)
        # Summarize results
        # summary_prompt = f"Analyze this data and summarize key insights: {result_data}"
        summary_prompt = f"Analyze this SQL query result and explain the insights in a human-friendly way: {result_data}"

        summary = llm.invoke(summary_prompt)

        return {
            "query": request.query,
            "sql": sql_query,
            "data": result_data,
            "summary": summary,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
