from lab07.contracts import ContractColumn,compare_schema
def test_missing_required_detected(spark):
    df=spark.createDataFrame([('x',)],'id string'); r=compare_schema(df.schema.fields,[ContractColumn('id','string',True),ContractColumn('license_number','bigint',True)]); assert not r['passed']; assert 'license_number' in r['missing_required']
