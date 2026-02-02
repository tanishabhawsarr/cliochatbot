# import json
# import struct
# import pyodbc
# import os

# from flask import Flask, request, jsonify
# from azure.identity import ClientSecretCredential
# from openai import AzureOpenAI
# from dotenv import load_dotenv

# load_dotenv()

# app = Flask(__name__)




# # ---------------- DB CONNECTION ----------------
# def get_db_connection():
#     credential = ClientSecretCredential(
#         tenant_id=os.getenv("AZURE_TENANT_ID"),
#         client_id=os.getenv("AZURE_CLIENT_ID"),
#         client_secret=os.getenv("AZURE_CLIENT_SECRET")
#     )

#     token = credential.get_token(
#         "https://database.windows.net/.default"
#     ).token

#     SQL_COPT_SS_ACCESS_TOKEN = 1256
#     token_bytes = token.encode("UTF-16-LE")
#     token_struct = struct.pack(
#         f"<I{len(token_bytes)}s",
#         len(token_bytes),
#         token_bytes
#     )

#     conn_str = (
#         "Driver={ODBC Driver 18 for SQL Server};"
#         f"Server=tcp:{os.getenv('DB_SERVER')},1433;"
#         f"Database={os.getenv('DB_NAME')};"
#         "Encrypt=yes;"
#         "TrustServerCertificate=yes;"
#         "Connection Timeout=30;"
#     )

#     return pyodbc.connect(
#         conn_str,
#         attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}
#     )

# # ---------------- SCHEMA ----------------
# def get_schema_info(cursor):
#     schema_info = {}
#     cursor.execute("""
#         SELECT TABLE_SCHEMA, TABLE_NAME
#         FROM INFORMATION_SCHEMA.TABLES
#         WHERE TABLE_TYPE='BASE TABLE'
#     """)

#     for schema, table in cursor.fetchall():
#         cursor.execute("""
#             SELECT COLUMN_NAME, DATA_TYPE
#             FROM INFORMATION_SCHEMA.COLUMNS
#             WHERE TABLE_SCHEMA=? AND TABLE_NAME=?
#         """, (schema, table))

#         schema_info[f"{schema}.{table}"] = [
#             {"name": c[0], "type": c[1]} for c in cursor.fetchall()
#         ]
#     return schema_info

# # ---------------- SQL GENERATION ----------------
# def generate_sql(question, schema_info, company_name):
#     client = AzureOpenAI(
#         api_key=os.getenv("AZURE_OPENAI_KEY"),
#         api_version="2024-12-01-preview",
#         azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
#     )

#     system_prompt = f"""
#         You are an expert SQL query generator for a Microsoft Fabric Warehouse
#         used by a law firm management system.

#         VERY IMPORTANT CONTEXT (NON-NEGOTIABLE):
#         - The database uses ONLY the '{company_name}' schema.
#         - ALL queryable objects are VIEWS, not tables.
#         - View names ALWAYS start with 'vw_'.
#         - Views must be referenced using this exact syntax:
#         FROM {company_name}.vw_ViewName
#         - NEVER use dbo, sys, or any other schema.
#         - NEVER use base tables.
#         - NEVER invent view or column names.
#         - Use ONLY views and columns provided in schema_info.

#         MANDATORY RULES (STRICT):
#         1. ALWAYS use table aliases (a, u, b, m).
#         2. ALWAYS reference columns using aliases.
#         3. Use ONLY views listed in schema_info.
#         4. NEVER invent columns or views.
#         5. Output MUST be a single valid SQL query.
#         6. SQL must be compatible with Microsoft Fabric Warehouse.
#         7. DO NOT include explanations, markdown, comments, or formatting.
#         8. If required data is missing from schema_info, return an EMPTY string.

#         DATE & TIME HANDLING (CRITICAL):
#         - If the user asks for "current year":
#         - ALWAYS calculate dates inside SQL.
#         - NEVER use placeholders like <today_date>.
#         - if talking about current year,Use:
#             a.Date BETWEEN DATEFROMPARTS(YEAR(GETDATE()), 1, 1) AND CAST(GETDATE() AS DATE)

#         -if talking about a specific year dont use betweem just use this- Use YEAR(a.Date) ONLY when a specific year is explicitly mentioned (e.g. "year 2025").
        
#         for the billable amount or hours take care of this:
#             take type = 'TimeEntry' and
#             Billable when non_billable = false
#             Non-billable when non_billable = true

#         AGGREGATION RULES:
#         - When using SUM or COUNT with non-aggregated columns, ALWAYS use GROUP BY.
#         - ORDER BY must match the aggregated column being selected.

#         VIEW USAGE RULES:
#         - Activities → {company_name}.vw_Activities a
#         - Users → {company_name}.vw_Users u
#         - Bills → {company_name}.vw_Bills b
#         - Matters → {company_name}.vw_Matters m

#         QUERY PATTERNS TO FOLLOW EXACTLY:

#         1. Total Billable Amount:
#         - Use SUM(a.Total_Amount) AS Billable_Amount
#         - Source: {company_name}.vw_Activities a

#         2. Total Non-Billable Amount:
#         - Use SUM(a.non_billable_amount) AS NonBillable_Amount
#         - Source: {company_name}.vw_Activities a

#         3. Billable & Non-Billable Hours:
#         - Use SUM(a.rounded_quantity_in_hours)
#         - Filter Type = 'TimeEntry'
#         - Filter non_billable = 'false' or 'true'

#         4. Billable / Non-Billable Hours by Job Title:
#         - JOIN {company_name}.vw_Activities a
#         WITH {company_name}.vw_Users u
#         ON a.User_Id = u.User_Id
#         - GROUP BY u.job_title
#         - ORDER BY SUM(a.rounded_quantity_in_hours) DESC

#         FINAL REMINDER:
#         - '{company_name}' schema usage is MANDATORY.
#         - Views only.
#         - Fabric SQL only.
#         """




#     user_prompt = f"""
#         Schema Info:
#         {json.dumps(schema_info)}

#         User Question:
#         {question}

#         IMPORTANT:
#         - Use ONLY the '{company_name}' schema.
#         - Use ONLY views from schema_info.
#         - Reference views like: {company_name}.vw_Activities
#         - Return ONLY a valid Fabric SQL query.
#         """



#     response = client.chat.completions.create(
#         model="gpt-4o-mini",
#         messages=[
#             {"role": "system", "content": system_prompt},
#             {"role": "user", "content": user_prompt}
#         ],
#         temperature=0
#     )

#     return (
#         response.choices[0].message.content
#         .replace("```sql", "")
#         .replace("```", "")
#         .replace("`", "")
#         .strip()
#     )


# # ---------------- EXECUTE SQL ----------------
# def execute_sql(sql, cursor):

#     cursor.execute(sql)
#     cols = [c[0] for c in cursor.description]
#     return [dict(zip(cols, row)) for row in cursor.fetchall()]

# # ---------------- HEALTH CHECK ----------------
# @app.route("/health", methods=["GET"])
# def health():
#     return {"status": "ok"}

# # ---------------- MAIN API ----------------
# @app.route("/query", methods=["POST", "GET"])
# def query():
#     try:
#         data = request.get_json(force=True)
#         print("Incoming data:", data)

#         question = data.get("question")
#         company_name = data.get("company_name")

#         if not question or not company_name:
#             return jsonify({
#                 "error": "Both 'question' and 'company_name' are required"
#             }), 400

#         conn = get_db_connection()
#         cursor = conn.cursor()

#         schema = get_schema_info(cursor)

#         sql = generate_sql(question, schema, company_name)
#         print("Generated SQL:", sql)

#         result = execute_sql(sql, cursor)

#         return jsonify({
#             "sql": sql,
#             "result": result
#         })

#     except Exception as e:
#         import traceback
#         traceback.print_exc()
#         return jsonify({"error": str(e)}), 500


# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=8000)


import json
import struct
import pyodbc
import os
import traceback

from flask import Flask, request, jsonify
from azure.identity import ClientSecretCredential
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# ---------------- DB CONNECTION ----------------
def get_db_connection():
    credential = ClientSecretCredential(
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET")
    )

    token = credential.get_token(
        "https://database.windows.net/.default"
    ).token

    SQL_COPT_SS_ACCESS_TOKEN = 1256
    token_bytes = token.encode("UTF-16-LE")
    token_struct = struct.pack(
        f"<I{len(token_bytes)}s",
        len(token_bytes),
        token_bytes
    )

    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{os.getenv('DB_SERVER')},1433;"
        f"Database={os.getenv('DB_NAME')};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=30;"
    )

    return pyodbc.connect(
        conn_str,
        attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}
    )

# ---------------- SCHEMA ----------------
def get_schema_info(cursor, company_name):
    schema_info = {}

    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = ?
          AND TABLE_NAME LIKE 'vw_%'
    """, (company_name,))

    views = cursor.fetchall()

    for schema, view in views:
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, (schema, view))

        schema_info[f"{schema}.{view}"] = [
            {"name": col, "type": dtype}
            for col, dtype in cursor.fetchall()
        ]

    return schema_info

# ---------------- SQL GENERATION ----------------
def generate_sql(question, schema_info, company_name):
    client = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_KEY"),
        api_version="2024-12-01-preview",
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
    )

    system_prompt =f"""
You are an expert SQL query generator for a Microsoft Fabric Warehouse
used with a Clio-based law firm management system.

You are provided `schema_info`.
`schema_info` is the ONLY source of truth for all views, columns,
data types, and relationships.

────────────────────────────────
CORE PLATFORM RULES (NON-NEGOTIABLE)
────────────────────────────────
• Use ONLY the '{company_name}' schema.
• ALL queryable objects are VIEWS only (vw_*).
• NEVER use dbo, sys, or base tables.
• NEVER invent views or columns.
• ALWAYS read schema_info before writing SQL.
• If required data is missing, return an EMPTY string.
• Do NOT use general knowledge outside schema_info.

────────────────────────────────
COLUMN NAME ENFORCEMENT (CRITICAL)
────────────────────────────────
• Column names are CASE-SENSITIVE.
• Copy column names EXACTLY as they appear in schema_info.
• Do NOT change casing, spelling, or underscores.
• Treat column names as literal tokens.
• If unsure of a column name, return an EMPTY string.

• For revenue-related questions → use vw_Activities.
• For client name → use vw_Users.Name ONLY if it exists in schema_info.
• Do NOT join clients using Matters.client_id unless schema_info explicitly allows it.

────────────────────────────────
SQL GENERATION RULES
────────────────────────────────
• SQL must be Microsoft Fabric Warehouse compatible (T-SQL).
• Output MUST be a single valid SQL query.
• ALWAYS use table aliases (a, u, b, m).
• ALWAYS reference columns using aliases.
• NEVER use SELECT *.
• NEVER use LIMIT or OFFSET (use TOP).

────────────────────────────────
STATUS, BILLING & TIME LOGIC
────────────────────────────────
• status values: 'Open', 'Closed', 'Pending'
• Is_Billed values: 'true', 'false'
• Time entries are identified by:
  a.Type = 'TimeEntry'

• Billable:
  a.non_billable = 'false'

• Non-billable:
  a.non_billable = 'true'

────────────────────────────────
USER / ATTORNEY / JOB TITLE QUERIES
────────────────────────────────
• For questions involving:
  "by user", "by attorney", "by job title":

  JOIN:
  vw_Activities a
  WITH vw_Users u
  ON a.User_Id = u.User_Id

• HOURS questions:
  use SUM(a.rounded_quantity_in_hours)
  with Type = 'TimeEntry'

• AMOUNT / REVENUE questions:
  use SUM(a.Total_Amount)

• GROUP BY:
  u.User_Id or u.job_title (based on question)

• ORDER BY aggregated value DESC

────────────────────────────────
DATE & TIME INTERPRETATION (MANDATORY)
────────────────────────────────
• "current", "today", "this year", "this month", "now"
  MUST ALWAYS be calculated using GETDATE().

Mappings:
• today:
  CAST(GETDATE() AS DATE)

• this year:
  column >= DATEFROMPARTS(YEAR(GETDATE()), 1, 1)
  AND column <= CAST(GETDATE() AS DATE)

• this month:
  column >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)
  AND column <  DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) + 1, 0)

• Hard-coded dates are allowed ONLY if the user explicitly mentions a year/date.

────────────────────────────────
DATE COLUMN SELECTION RULE
────────────────────────────────
• Use the date column that represents the business event.
• Do NOT use updated_at as a substitute for business events.

• If date columns are strings:
  use TRY_CONVERT() or TRY_CAST().
• Use date ranges.
• Do NOT use YEAR(), MONTH(), DAY() on columns.

────────────────────────────────
AGGREGATION RULES
────────────────────────────────
• All non-aggregated columns MUST appear in GROUP BY.
• ORDER BY must reference aggregated expressions.

────────────────────────────────
STANDARD VIEW ALIASES
────────────────────────────────
• Activities → {company_name}.vw_Activities a
• Users      → {company_name}.vw_Users u
• Bills      → {company_name}.vw_Bills b
• Matters    → {company_name}.vw_Matters m

────────────────────────────────
FINAL INSTRUCTION
────────────────────────────────
Generate a SQL query that correctly answers the user's question.
Return ONLY the SQL query.
If schema_info does not support the question,
return an EMPTY string.
"""


    user_prompt = f"""
Schema Info:
{json.dumps(schema_info)}

User Question:
{question}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0
    )

    return (
        response.choices[0].message.content
        .replace("```sql", "")
        .replace("```", "")
        .replace("`", "")
        .strip()
    )

# ---------------- EXECUTE SQL ----------------
def execute_sql(sql, cursor):
    cursor.execute(sql)

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    if not rows:
        return {"type": "empty", "data": []}

    if len(columns) == 1 and len(rows) == 1:
        return {"type": "scalar", "data": rows[0][0]}

    return {
        "type": "table",
        "columns": columns,
        "data": [dict(zip(columns, row)) for row in rows]
    }

# ---------------- HUMAN ANSWER ----------------
def generate_human_answer(question, result):
    client = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_KEY"),
        api_version="2024-12-01-preview",
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
    )

    system_prompt = """
You are a law firm analytics assistant.

STRICT RULES:
• Use ONLY the provided result data
• Do NOT invent or infer anything
• Do NOT mention SQL, queries, or databases
• If result type is 'scalar', explain the number
• If result type is 'table', summarize factually
• If result is empty, say no data was found
• Keep the response concise and professional

FORMAT RULES:
• If result type is 'scalar', return one clear sentence
• If result type is 'table':
  - write the first sentence exlaining the answer or question relation 
  - Format each row as a bullet using 'numbers'
  - Strictly Do NOT use newline or /n characters
  - Separate bullets with two spaces
• If result is empty, say no data was found
"""

    user_prompt = f"""
Question:
{question}

Result:
{json.dumps(result, default=str)}

Generate the final answer.
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0
    )

    return response.choices[0].message.content.strip()

# ---------------- HEALTH CHECK ----------------
@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}

# ---------------- MAIN API ----------------
@app.route("/query", methods=["POST","GET"])
def query():
    try:
        data = request.get_json(force=True)

        question = data.get("question")
        company_name = data.get("company_name")

        if not question or not company_name:
            return jsonify({
                "error": "Both 'question' and 'company_name' are required"
            }), 400

        conn = get_db_connection()
        cursor = conn.cursor()

        schema = get_schema_info(cursor, company_name)

        sql = generate_sql(question, schema, company_name)

        print("Generated SQL:", sql)

        if not sql:
            return jsonify({
                "answer": "I couldn’t find enough data to answer that question."
            })

        result = execute_sql(sql, cursor)

        if result["type"] == "empty":
            return jsonify({
                "answer": "No data was found for this question."
            })

        human_answer = generate_human_answer(question, result)

        return jsonify({"answer": human_answer})

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ---------------- RUN ----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
