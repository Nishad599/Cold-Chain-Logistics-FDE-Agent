import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Clean dataset column -> messy 2000s legacy column in tbl_sc_fleet_hist_raw
# (lowercase because Postgres folds unquoted identifiers to lowercase).
# Shared by the batch ingest and the Kafka consumer.
LEGACY_MAPPING = {
    'timestamp': 'ts_utc',
    'vehicle_gps_latitude': 'v_lat',
    'vehicle_gps_longitude': 'v_lon',
    'iot_temperature': 'iot_temp_val_c',
    'cargo_condition_status': 'cgo_cond_cd',
    'risk_classification': 'risk_cls_txt',
    'delay_probability': 'delay_prob_dec',
    'port_congestion_level': 'prt_cng_lvl',
    'route_risk_level': 'rt_rsk_idx'
}


def get_engine(user: str, password: str):
    """One place to build a Postgres connection; each caller passes its own (least-privilege) login."""
    # URL.create escapes special characters in passwords (e.g. '@', '!') safely
    return create_engine(URL.create(
        "postgresql+psycopg",
        username=user,
        password=password,
        host=os.getenv("PG_HOST", "127.0.0.1"),
        port=int(os.getenv("PG_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "fde"),
    ))
