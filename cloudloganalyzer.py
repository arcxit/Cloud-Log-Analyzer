import os, json, re, csv, datetime as dt
import pandas as pd

# Optional OCI upload
def oci_upload(path, cfg):
    if not cfg.get("oci_enabled"):
        return False, "OCI disabled"
    try:
        import oci
        config = {
            "user": cfg["user"],
            "key_file": cfg["key_file"],
            "fingerprint": cfg["fingerprint"],
            "tenancy": cfg["tenancy"],
            "region": cfg["region"]
        }
        object_storage = oci.object_storage.ObjectStorageClient(config)
        namespace = cfg["namespace"]
        bucket = cfg["bucket_name"]
        name = os.path.basename(path)
        with open(path, "rb") as f:
            object_storage.put_object(namespace, bucket, name, f)
        return True, f"Uploaded {name} to OCI bucket {bucket}"
    except Exception as e:
        return False, f"OCI upload failed: {e}"

def parse_logs(log_dir="logs", out_csv="report.csv"):
    rows = []
    pattern_err = re.compile(r"(ERROR|Error|Exception|FAIL|Failed)")
    os.makedirs(log_dir, exist_ok=True)
    for root, _, files in os.walk(log_dir):
        for fn in files:
            if not fn.lower().endswith((".log", ".txt")): 
                continue
            fp = os.path.join(root, fn)
            with open(fp, "r", errors="ignore") as f:
                for i, line in enumerate(f, start=1):
                    if pattern_err.search(line):
                        rows.append({"file": fn, "line_no": i, "snippet": line.strip()})
    df = pd.DataFrame(rows)
    if df.empty:
        print("No anomalies found.")
        return None
    df.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv} with {len(df)} rows.")
    return out_csv

def main():
    # Load optional config
    cfg = {}
    if os.path.exists("config.json"):
        with open("config.json") as f:
            cfg = json.load(f)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = f"report_{ts}.csv"
    path = parse_logs(log_dir="logs", out_csv=out_csv)
    if path:
        ok, msg = oci_upload(path, cfg)
        print(msg)

if __name__ == "__main__":
    main()
