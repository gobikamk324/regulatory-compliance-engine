import psycopg2
from elasticsearch import Elasticsearch

# Connect to PostgreSQL (cupboard)
conn = psycopg2.connect(
    dbname="compliance_db",
    user="postgres",
    password="gobikA@1234",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Connect to Elasticsearch (search robot)
es = Elasticsearch("http://localhost:9201", basic_auth=("elastic", "*at4KrvSi=*8aqqszayv"))

# Get data from PostgreSQL
cur.execute("SELECT id, amount, date, sender, receiver FROM transactions")
rows = cur.fetchall()

# Push each row into Elasticsearch
for row in rows:
    doc = {
        "id": row[0],
        "amount": row[1],
        "date": str(row[2]),   # convert date to string
        "sender": row[3],
        "receiver": row[4]
    }
    es.index(index="transactions", document=doc)

print("Data copied successfully!")
