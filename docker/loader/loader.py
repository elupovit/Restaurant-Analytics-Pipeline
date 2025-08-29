import os
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.mssql import NVARCHAR, FLOAT, INTEGER
from urllib.parse import quote_plus

# ==== RDS CONFIG ====
HOST = "restaurant-sqlserver-dev.cobcyc8q8zk3.us-east-1.rds.amazonaws.com"
PORT = 1433
DB   = "RestaurantAnalytics"
USER = "admin"
PASS = "l:8:u>IF!WRN-g[BW#->)00mx[Y0"

RAW = "/data"
FILES = {
    "order_items":        f"{RAW}/order_items.csv",
    "order_item_options": f"{RAW}/order_item_options.csv",
    "date_dim":           f"{RAW}/date_dim.csv",
}
ONLY = os.environ.get("ONLY", "").strip() 
CHUNK = 5000

def conn_str():
    return f"mssql+pymssql://{USER}:{quote_plus(PASS)}@{HOST}:{PORT}/{DB}"

def norm_cols(df):
    df.columns = (df.columns.str.strip().str.lower()
                  .str.replace(" ", "_").str.replace("[^0-9a-zA-Z_]", "", regex=True))
    return df

def build_dtype(df):
    qty_ints = {"item_quantity", "option_quantity"}
    price_floats = {"item_price", "option_price"}
    d = {}
    for c in df.columns:
        if c in qty_ints:       d[c] = INTEGER()
        elif c in price_floats: d[c] = FLOAT()
        else:                   d[c] = NVARCHAR(4000)
    return d

def ensure_table(engine, table, path):
    """Create table only if it doesn't already exist."""
    header = pd.read_csv(path, nrows=0, encoding="utf-8")
    header = norm_cols(header)
    dtype = build_dtype(header)

    insp = inspect(engine)
    if insp.has_table(table, schema="dbo"):
        # Table exists; do not attempt to create
        return dtype

    with engine.begin() as conn:
        header.to_sql(table, conn, schema="dbo", if_exists="fail",
                      index=False, dtype=dtype)
    return dtype

def load_table(engine, table, path, dtype):
    total = 0
    for chunk in pd.read_csv(path, chunksize=CHUNK, low_memory=False, encoding='utf-8'):
        chunk = norm_cols(chunk)
        for c in ("item_quantity","option_quantity","item_price","option_price"):
            if c in chunk.columns:
                chunk[c] = pd.to_numeric(chunk[c], errors="coerce")
        chunk = chunk.where(chunk.notnull(), None)
        with engine.begin() as conn:
            chunk.to_sql(table, conn, schema='dbo', if_exists='append',
                         index=False, dtype=dtype)
        total += len(chunk)
        print(f"{table}: committed {total:,} rows so far")
    print(f"✅ {table}: {total:,} rows inserted.")

def main():
    for t,p in FILES.items():
        if not os.path.exists(p):
            raise FileNotFoundError(f"Missing CSV: {p}")
    engine = create_engine(conn_str(), pool_pre_ping=True)
    targets = [t.strip() for t in ONLY.split(",") if t.strip()] or list(FILES.keys())   

    #for t,p in FILES.items():
    for t in targets:
        p = FILES[t]
        print(f"→ Preparing {t}")
        dtypes = ensure_table(engine, t, p)
        print(f"→ Loading {t} from {p}")
        load_table(engine, t, p, dtypes)
    engine.dispose()
    print("🎉 All done.")

if __name__ == "__main__":
    main()
