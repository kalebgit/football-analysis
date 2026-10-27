import boto3, json
from botocore.config import Config
from botocore.exceptions import ClientError
import nflreadpy as nfl
import polars as pl
from tqdm import tqdm




def s3_key(*parts): 
    return "/".join(str(p).strip("/") for p in parts)


def upload_df(df: pl.DataFrame, key: str): 
    uri = f"s3://{BUCKET_NAME}/{key}"
    df.write_parquet(
        uri,
        #muy importante este parametro porque polars usa 
        #creenciales por defcto primero en varaibles
        #de entorno, despues variables del codigio como 
        #AWS_PROFILE, en el archivo ~/.aws/config
        credential_provider=pl.CredentialProviderAWS(
            profile_name=PROFILE, 
            region_name=REGION
        )
    )
    


def load_to_s3(name, df):
    with tqdm(
        total=1,
        unit_scale=False,
        unit="dataset",
        desc="Subiendo dataset a S3 amazon",
        position=0 #para las barras andidadas
    ) as pbar:
        base = ["raw", name]
        if "season" in df.columns: 
            for season in tqdm(
                    df["season"].unique().sort(), 
                    unit="season dataset", 
                    desc="Cargando seasons",
                    position=1,
                    #para no mostrar la sub barra de seasons
                    leave=False):
                part = df.filter(pl.col("season") == season).drop("season")
                key = s3_key(*base, f"season={season}", "data.parquet")
                upload_df(part, key)
            pbar.update(1)
        else: 
            key = s3_key(*base, "data.parquet")
            upload_df(df, key)
            pbar.update(1)


def ensure_bucket(s3, bucket, region="us-east-1"):
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket {bucket} ya existe")
    except ClientError as e: 
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchBucket"): 
            if region == "us-east-1": 
                s3.create_bucket(Bucket=bucket)
            else: 
                s3.create_bucket(
                    Bucket=bucket, 
                    CreateBucketConfiguration={
                        "LocationConstraint": region
                    }
                )
            print(f"Bucket {bucket} creado")
        elif code == "403": 
            raise RuntimeError(f"El bucket {bucket} existe pero es de otra cuenta (los nombres son globales)")
        else:
            raise

    
    


BUCKET_NAME = "football-analysis-2026-kaleb"
REGION = "us-east-2"
PROFILE = "dev"

cfg = Config(retries={
    "max_attempts": 10, 
    "mode": "adaptive"
})

session = boto3.Session(
    profile_name=PROFILE, 
    region_name=REGION
)

s3 = session.client(
    "s3",
    config=cfg,
)





ensure_bucket(s3, BUCKET_NAME, REGION)

SEASONS=list(range(2016, 2026))
pbp = nfl.load_pbp(SEASONS)
schedules = nfl.load_schedules(SEASONS)
rosters = nfl.load_rosters(SEASONS)
player_stats = nfl.load_player_stats(SEASONS)
contracts = nfl.load_contracts()
participation = nfl.load_participation()
ftn_charting = nfl.load_ftn_charting()


datasets = {
    "pbp": pbp, 
    "schedules": schedules, 
    "rosters": rosters, 
    "player_stats": player_stats, 
    "contracts": contracts, 
    "participation": participation, 
    "ftn_charting": ftn_charting
}

for name, dataset in datasets.items(): 
    load_to_s3(name, dataset)
    