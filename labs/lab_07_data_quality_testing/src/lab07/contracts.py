from dataclasses import dataclass
from pathlib import Path
import yaml
@dataclass(frozen=True)
class ContractColumn: name:str; type:str; required:bool

def load_contract(path): return yaml.safe_load(Path(path).read_text(encoding="utf-8"))
def contract_columns(c): return [ContractColumn(x["name"],x["type"],bool(x.get("required"))) for x in c["columns"]]
def _norm(t): return {"long":"bigint","integer":"int"}.get(t.replace(" ","").lower(),t.replace(" ","").lower())
def compare_schema(actual_fields, expected_columns, strict_columns=False):
    a={f.name:_norm(f.dataType.simpleString()) for f in actual_fields}; e={x.name:_norm(x.type) for x in expected_columns}; req={x.name for x in expected_columns if x.required}
    missing=sorted(req-set(a)); unexpected=sorted(set(a)-set(e)) if strict_columns else []; mism=[{"column":n,"expected":e[n],"actual":a[n]} for n in sorted(set(a)&set(e)) if a[n]!=e[n]]
    return {"passed":not missing and not unexpected and not mism,"missing_required":missing,"unexpected":unexpected,"mismatched_types":mism}
