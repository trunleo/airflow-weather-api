import os

def verify_paths():
    # Mimic the logic in transform_data.py
    # Since we are running this from /Users/trung.tran/Library/Mobile Documents/com~apple~CloudDocs/Documents/Personal/Project/airflow/dags/marketprice/
    # (assuming we run it there or provide the absolute path to transform_data.py)
    
    dag_file = "/Users/trung.tran/Library/Mobile Documents/com~apple~CloudDocs/Documents/Personal/Project/airflow/dags/marketprice/transform_data.py"
    dag_path = os.path.dirname(dag_file)
    
    sql_path = os.path.join(dag_path, "sql/mapping_list.sql")
    csv_path = os.path.join(dag_path, "data/mapping_list.csv")
    
    print(f"Checking SQL path: {sql_path}")
    if os.path.exists(sql_path):
        print("✅ SQL file exists")
    else:
        print("❌ SQL file NOT found")
        
    print(f"Checking CSV path: {csv_path}")
    if os.path.exists(csv_path):
        print("✅ CSV file exists")
    else:
        print("❌ CSV file NOT found")

if __name__ == "__main__":
    verify_paths()
