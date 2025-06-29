from fastapi import FastAPI, HTTPException, UploadFile, File, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Date, JSON, ForeignKey
from datetime import date
import uuid
from dotenv import dotenv_values
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import json
import psycopg2.extras
import requests
import google.generativeai as genai

config = dotenv_values('.env')

def ensure_database_exists():
    # STEP 1: Connect to default 'postgres' DB
    conn = psycopg2.connect(
        host=config['HOSTNAME'],
        dbname='postgres',  # Always connect to this first
        user=config['USERNAME'],
        password=config['PWD'],
        port=config['PORT']
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  # Required for CREATE DATABASE
    cursor = conn.cursor()

    # STEP 2: Check if the target DB exists
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{config['DATABASE']}'")
    exists = cursor.fetchone()

    if not exists:
        # STEP 3: Create the database
        cursor.execute(f"CREATE DATABASE {config['DATABASE']}")
        print(f"Database {config['DATABASE']} created successfully.")
    cursor.close()
    conn.close()

    # STEP 4: Now connect to the target database to create tables and insert data
    db_conn = psycopg2.connect(
        host=config['HOSTNAME'],
        dbname=config['DATABASE'],
        user=config['USERNAME'],
        password=config['PWD'],
        port=config['PORT']
    )
    db_cursor = db_conn.cursor()

    # Create tables if not exist
    db_cursor.execute("""
    CREATE TABLE IF NOT EXISTS suppliers (
        supplier_id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL,
        country VARCHAR NOT NULL,
        compliance_score INTEGER NOT NULL,
        contract_terms JSON,
        last_audit DATE
    );
    """)
    db_cursor.execute("""
    CREATE TABLE IF NOT EXISTS compliance_records (
        id SERIAL PRIMARY KEY,
        supplier_id INTEGER REFERENCES suppliers(supplier_id) ON DELETE CASCADE,
        metric VARCHAR NOT NULL,
        date_recorded DATE NOT NULL,
        result VARCHAR,
        status VARCHAR
    );
    """)

    # Only insert data if database was just created
    if not exists:
        df = pd.read_excel("./assets/Task_Supplier_Data.xlsx")
        for _, row in df.iterrows():
            name = row['name']
            country = row['country']
            compliance_score = int(row['compliance_score'])
            contract_terms = row['contract_terms']
            if isinstance(contract_terms, str):
                try:
                    contract_terms = json.loads(contract_terms)
                except json.JSONDecodeError:
                    contract_terms = {"raw": contract_terms}
            elif isinstance(contract_terms, dict):
                pass
            else:
                contract_terms = {"raw": str(contract_terms)}
            last_audit = row['last_audit']
            if pd.isna(last_audit):
                last_audit = None
            else:
                last_audit = pd.to_datetime(last_audit).date()
            db_cursor.execute("""
                INSERT INTO suppliers (name, country, compliance_score, contract_terms, last_audit)
                VALUES (%s, %s, %s, %s, %s)
            """, (name, country, compliance_score, json.dumps(contract_terms), last_audit))

        compliance_df = pd.read_excel("./assets/Task_Compliance_Records.xlsx")
        for _, row in compliance_df.iterrows():
            supplier_id = int(row['supplier_id'])
            metric = row['metric']
            date_recorded = pd.to_datetime(row['date_recorded']).date()
            result = row['result']
            status = row['status']
            if status == 'Pass':
                status = 'Compliant'
            elif status == 'Fail':
                status = 'Non-Compliant'
            db_cursor.execute("""
                INSERT INTO compliance_records (supplier_id, metric, date_recorded, result, status)
                VALUES (%s, %s, %s, %s, %s)
            """, (supplier_id, metric, date_recorded, result, status))

    db_conn.commit()
    db_cursor.close()
    db_conn.close()

ensure_database_exists()

conn = psycopg2.connect(
    host=config['HOSTNAME'],
    dbname=config['DATABASE'],
    user=config['USERNAME'],
    password=config['PWD'],
    port=config['PORT']
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/getsuppliers")
async def get_suppliers():
    db_cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    db_cursor.execute("SELECT * FROM suppliers")
    suppliers = db_cursor.fetchall()
    for supplier in suppliers:
        supplier["supplierId"] = supplier.get("supplier_id")
        supplier.pop("supplier_id", None)  # Remove the original key
        supplier["complianceScore"] = supplier.get("compliance_score")
        supplier.pop("compliance_score", None)  # Remove the original key
        supplier["lastAuditDate"] = supplier.get("last_audit")
        supplier.pop("last_audit", None)
        ct = supplier.get("contract_terms")
        if isinstance(ct, dict) and "raw" in ct and isinstance(ct["raw"], str):
            raw_str = ct["raw"].replace("'", '"')
            try:
                supplier["contractTerms"] = json.loads(raw_str)
            except Exception:
                supplier["contractTerms"] = ct["raw"]  # fallback to raw string
        elif isinstance(ct, str):
            try:
                supplier["contractTerms"] = json.loads(ct)
            except Exception:
                pass
        supplier.pop("contract_terms", None)
    db_cursor.close()
    return suppliers

@app.get("/getsupplier/{supplier_id}")
async def get_supplier(supplier_id: int):
    db_cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    db_cursor.execute("SELECT * FROM suppliers WHERE supplier_id = %s", (supplier_id,))
    supplier = db_cursor.fetchone()
    if not supplier:
        raise HTTPException(status_code=404, detail="Supplier not found")
    
    supplier["supplierId"] = supplier.get("supplier_id")
    supplier.pop("supplier_id", None)
    supplier["complianceScore"] = supplier.get("compliance_score")
    supplier.pop("compliance_score", None)
    supplier["lastAuditDate"] = supplier.get("last_audit")
    supplier.pop("last_audit", None)
    ct = supplier.get("contract_terms")
    if isinstance(ct, dict) and "raw" in ct and isinstance(ct["raw"], str):
        raw_str = ct["raw"].replace("'", '"')
        try:
            supplier["contractTerms"] = json.loads(raw_str)
        except Exception:
            supplier["contractTerms"] = ct["raw"]
    elif isinstance(ct, str):
        try:
            supplier["contractTerms"] = json.loads(ct)
        except Exception:
            pass
    supplier.pop("contract_terms", None)
    db_cursor.close()
    return supplier

@app.post("/addsupplier")
async def add_supplier(supplier: dict):
    db_cursor = conn.cursor()
    try:
        contract_terms_raw = supplier.get('contractTerms', {}).get('raw')
        # Always store as JSON string
        db_cursor.execute("""
            INSERT INTO suppliers (name, country, compliance_score, contract_terms, last_audit)
            VALUES (%s, %s, %s, %s, %s) RETURNING supplier_id
        """, (
            supplier['name'],
            supplier['country'],
            supplier['complianceScore'],
            json.dumps(contract_terms_raw),
            supplier.get('lastAuditDate')
        ))
        supplier_id = db_cursor.fetchone()[0]
        conn.commit()
        return {"supplierId": supplier_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db_cursor.close()

@app.post("/addcompliancerecord")
async def add_compliance_record(record: dict):
    db_cursor = conn.cursor()
    try:
        db_cursor.execute("""
            INSERT INTO compliance_records (supplier_id, metric, date_recorded, result, status)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            record['supplierId'],
            record['metric'],
            record['dateRecorded'],
            record['result'],
            record['status']
        ))
        conn.commit()
        return {"message": "Compliance record added successfully"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db_cursor.close()

@app.get("/getcompliancerecords/{supplier_id}")
async def get_compliance_records(supplier_id: int):
    db_cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    db_cursor.execute("SELECT * FROM compliance_records WHERE supplier_id = %s", (supplier_id,))
    records = db_cursor.fetchall()
    for record in records:
        record["id"] = record.get("id")
        record["supplierId"] = record.get("supplier_id")
        record.pop("supplier_id", None)
        record["dateRecorded"] = record.get("date_recorded")
        record.pop("date_recorded", None)
    db_cursor.close()
    return records

@app.get("/getaiinsights")
async def get_ai_insights():
    genai.configure(api_key="AIzaSyDzDA1qOtw5f1Exnj9fEGyADEwwmOfVZP0")
    ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=AIzaSyDzDA1qOtw5f1Exnj9fEGyADEwwmOfVZP0"
    db_cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    db_cursor.execute("SELECT * FROM compliance_records")
    records = db_cursor.fetchall()
    db_cursor.close()
    for record in records:
        if isinstance(record.get("date_recorded"), date):
            record["date_recorded"] = record["date_recorded"].isoformat()
    context = "You are a compliance insights assistant. Analyze the following compliance records and provide insights. You can change contract terms to improve compliance."
    user_data = json.dumps(records, indent=2)
    model = genai.GenerativeModel("gemini-2.0-flash")
    response = model.generate_content([
        {"role": "user", "parts": [f"{context}\nCompliance Records:\n{user_data}"]}
    ])

    return {"insights": response.text}