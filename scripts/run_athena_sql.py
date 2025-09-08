import os, sys, time, boto3

def run_athena(sql_path: str, database: str = "stormevents") -> None:
    region = os.getenv("AWS_REGION", "us-east-2")
    output = os.getenv("ATHENA_OUTPUT", "s3://YOUR-BUCKET/athena-output/")

    athena = boto3.client("athena", region_name=region)
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    q = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output},
    )
    qid = q["QueryExecutionId"]
    print(f"Started Athena query: {qid}")

    while True:
        res = athena.get_query_execution(QueryExecutionId=qid)
        s = res["QueryExecution"]["Status"]["State"]
        if s in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            print("Status:", s)
            if s != "SUCCEEDED":
                reason = res["QueryExecution"]["Status"].get("StateChangeReason", "")
                print("Reason:", reason)
            break
        time.sleep(2)

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "infra/sql/athena_create_table.sql"
    run_athena(path)
