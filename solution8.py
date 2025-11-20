from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
import csv
import os
import sys

# Configuration
ASTRA_CLIENT_ID_PLACEHOLDER = "GXoDIDlRTsyPUcpliNclDYrH"
ASTRA_TOKEN_SECRET = "S5G-aL0ikUBXmIWO2qGe29ycpTqaEFfcLJiA2j5EKx,AUDKa,BoOxs8GbiNhQZbUZDxWNkl7IQqN5RBGpGbzgenJ8.mX+QkLW6gGAY_Gs-Gw7UNMn_xWI3-s8DvZy42T"
SECURE_CONNECT_BUNDLE_PATH = "secure-connect-assignment8db.zip"
KEYSPACE_NAME = "default_keyspace"


class CassandraDB():  

    def __init__(self):
        self.session = None
        self.cluster = None
        self.CSV_FIELDS = ['id', 'gender', 'age', 'number_of_kids']
        self.CSV_FILE = "data/customers.csv"

    # Function to connect to Database
    # Connecting to the DataStax Astra DB using the Secure Connect Bundle
    def connect(self):        
        print("\n[INFO] Establishing connection to Cassandra (Astra DB)...")

        try:
            if not os.path.exists(SECURE_CONNECT_BUNDLE_PATH):
                raise FileNotFoundError(
                    f"Secure bundle not found at path: {SECURE_CONNECT_BUNDLE_PATH}"
                )
            cloud_config = {'secure_connect_bundle': SECURE_CONNECT_BUNDLE_PATH}
            auth_provider = PlainTextAuthProvider(ASTRA_CLIENT_ID_PLACEHOLDER, ASTRA_TOKEN_SECRET)
            self.cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            self.session = self.cluster.connect(KEYSPACE_NAME)
            print(f"[SUCCESS] Connected to keyspace '{KEYSPACE_NAME}'.")
        except Exception as e:
            print(f"[ERROR] Unable to connect to Astra DB: {e}")
            print("Please verify bundle path and DB status before retrying.")
            self.session = None
    
    
    def close(self):        
        if self.cluster:
            print("\n[INFO] Closing Cassandra connection...")
            self.cluster.shutdown()

    # Function to create the Customer Table with indices
    def create_table(self):        
        if not self.session:
            print("[WARNING] Cannot create table — no database connection.")
            return

        table_query = f"""
            CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.customer (
                id int,
                gender text,
                age int,
                number_of_kids int,
                PRIMARY KEY (id)
            );
        """
        gender_index = f"CREATE INDEX IF NOT EXISTS ON {KEYSPACE_NAME}.customer (gender);"
        age_index = f"CREATE INDEX IF NOT EXISTS ON {KEYSPACE_NAME}.customer (age);"

        try:
            self.session.execute(table_query)
            self.session.execute(gender_index)
            self.session.execute(age_index)
            print("[SUCCESS] Customer table and indexes have been created.")
        except Exception as e:
            print(f"[ERROR] Table creation failed: {e}")

    # Function to load data from CSV into table
    def load(self):        
        if not self.session:
            print("[WARNING] Cannot load data — no active database session.")
            return
        print(f"\n[INFO] Starting CSV import from: {self.CSV_FILE}")

        insert_query = f"""
            INSERT INTO {KEYSPACE_NAME}.customer (id, gender, age, number_of_kids)
            VALUES (?, ?, ?, ?)
        """

        try:
            prepared = self.session.prepare(insert_query)
            batch_rows = []

            with open(self.CSV_FILE, "r", encoding="utf-8") as file:
                reader = csv.DictReader(file, fieldnames=self.CSV_FIELDS)

                first_row = next(reader)
                if str(first_row.get('id', '')).strip() != 'id':
                    try:
                        batch_rows.append((
                            int(first_row["id"]),
                            first_row["gender"].strip(),
                            int(first_row["age"]),
                            int(first_row["number_of_kids"])
                        ))
                    except Exception:
                        pass

                for row in reader:
                    try:
                        batch_rows.append((
                            int(row["id"]),
                            row["gender"].strip(),
                            int(row["age"]),
                            int(row["number_of_kids"])
                        ))
                    except ValueError:
                        print(f"[SKIP] Invalid row encountered: {row}")
                        continue

            for entry in batch_rows:
                self.session.execute(prepared, entry)
            print(f"[SUCCESS] Loaded {len(batch_rows)} records into Cassandra.")
        except FileNotFoundError:
            print(f"[ERROR] CSV file missing: {self.CSV_FILE}")
        except Exception as e:
            print(f"[ERROR] Failed to load CSV data: {e}")

    # Funtion that returns the age of the customer whose id is 979863
    def query_1(self):        
        if not self.session:
            print("[WARNING] Cannot execute query — database not connected.")
            return

        target_id = 979863
        query = f"SELECT age FROM {KEYSPACE_NAME}.customer WHERE id = %s"

        try:
            print(f"\n[QUERY] Fetching age for customer ID: {target_id}")
            result = self.session.execute(query, (target_id,))
            row = result.one()

            if row:
                print(f"[RESULT] Customer age: {row.age}")
                return row.age
            else:
                print("[RESULT] No matching customer found.")
                return None

        except Exception as e:
            print(f"[ERROR] Query execution failed: {e}")
            return None

    # Funtion that Retrieves all male customers aged 25 or 35
    def query_2(self):        
        if not self.session:
            print("[WARNING] Cannot execute query — no database connection.")
            return

        query = f"""
            SELECT id, gender, age, number_of_kids
            FROM {KEYSPACE_NAME}.customer
            WHERE gender = 'MALE' AND age IN (25, 35)
            ALLOW FILTERING;
        """

        try:
            print("\n[QUERY] Fetching male customers (age = 25 or 35)...")
            rows = list(self.session.execute(query))

            print(f"[INFO] Records found: {len(rows)}")
            for r in rows:
                print(f" → ID:{r.id}, Age:{r.age}, Gender:{r.gender}, Kids:{r.number_of_kids}")

            return rows

        except Exception as e:
            print(f"[ERROR] Query execution failed: {e}")
            return []


# main function
if __name__ == '__main__':

    if SECURE_CONNECT_BUNDLE_PATH == r"C:\path\to\secure-connect-sarthakdb.zip":
        print("\n[IMPORTANT] Please update SECURE_CONNECT_BUNDLE_PATH to your actual file path.")
    else:
        client = CassandraDB()
        client.connect()

        if client.session:
            client.create_table()
            client.load()
            client.query_1()
            client.query_2()

        client.close()
