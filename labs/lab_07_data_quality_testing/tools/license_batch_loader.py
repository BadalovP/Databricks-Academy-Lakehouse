import argparse, json, os, time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
DATASET_ID="r5kz-chrr"; DOMAIN="https://data.cityofchicago.org"
def _get(url,token=None):
    h={"Accept":"application/json","User-Agent":"databricks-academy-lab07/1.0"}
    if token: h["X-App-Token"]=token
    with urlopen(Request(url,headers=h),timeout=60) as r: return json.loads(r.read().decode())
def dataset_updated_at(token=None):
    x=_get(f"{DOMAIN}/api/views/{DATASET_ID}",token); ts=x.get("rowsUpdatedAt") or x.get("dataUpdatedAt") or x.get("metadataUpdatedAt")
    return datetime.fromtimestamp(int(ts),timezone.utc).isoformat() if ts else None
def download(output_dir,source_from_date="2024-01-01",source_to_date=None,max_rows=300000,page_size=50000):
    out=Path(output_dir); out.mkdir(parents=True,exist_ok=True); token=os.getenv("CHICAGO_APP_TOKEN"); updated=dataset_updated_at(token); api=f"{DOMAIN}/resource/{DATASET_ID}.json"; off=0; total=0; files=[]; batch=1
    clauses=[]
    if source_from_date: clauses.append(f"date_issued >= '{source_from_date}T00:00:00.000'")
    if source_to_date: clauses.append(f"date_issued <= '{source_to_date}T23:59:59.999'")
    where=" AND ".join(clauses) if clauses else None
    while max_rows<=0 or total<max_rows:
        limit=page_size if max_rows<=0 else min(page_size,max_rows-total); params={"$limit":limit,"$offset":off,"$order":"date_issued ASC, license_id ASC, id ASC"}
        if where: params["$where"]=where
        rows=_get(api+"?"+urlencode(params),token)
        if not rows: break
        bid=f"batch_{batch:04d}"; now=datetime.now(timezone.utc).isoformat(); path=out/f"{bid}.json"
        with path.open('w',encoding='utf-8') as f:
            for i,row in enumerate(rows):
                row['_source_batch_id']=bid; row['_source_offset']=off+i; row['_source_api']=api; row['_source_dataset_id']=DATASET_ID; row['_source_dataset_updated_at']=updated; row['_ingested_at']=now; row['_fixture_kind']=None
                f.write(json.dumps(row,ensure_ascii=False)+'\n')
        files.append(str(path)); n=len(rows); total+=n; off+=n; batch+=1
        if n<limit: break
        time.sleep(.1)
    return {'downloaded_rows':total,'source_dataset_updated_at':updated,'files':files}
if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--output-dir',required=True); p.add_argument('--source-from-date',default='2024-01-01'); p.add_argument('--source-to-date'); p.add_argument('--max-rows',type=int,default=300000); a=p.parse_args(); print(json.dumps(download(a.output_dir,a.source_from_date,a.source_to_date,a.max_rows),indent=2))
