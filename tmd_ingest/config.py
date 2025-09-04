from __future__ import annotations
import os
from airflow.models import Variable

def get_creds():
    API_URL = Variable.get("api_url", default_var=os.getenv("TMD_API_URL"))
    uid = Variable.get("tmd_api_uid", default_var=os.getenv("TMD_API_UID"))
    ukey = Variable.get("tmd_api_ukey", default_var=os.getenv("TMD_API_UKEY"))
    if not uid or not ukey:
        raise RuntimeError("Missing creds: tmd_api_url/tmd_api_uid/tmd_api_ukey")
    return uid, ukey