from demo2.data_generation import generate_v1_orders
from demo2.hashing import canonical_order_hash, canonical_order_json


def test_hash_ignores_runtime_metadata_and_dictionary_order():
    row = generate_v1_orders(1)[0]
    changed_metadata = {
        **row,
        "_ingested_at": "2099-01-01T00:00:00Z",
        "_batch_loaded_at": "2099-01-01T00:00:00Z",
    }
    reversed_row = dict(reversed(list(row.items())))
    assert canonical_order_hash(row) == canonical_order_hash(changed_metadata)
    assert canonical_order_hash(row) == canonical_order_hash(reversed_row)


def test_hash_changes_with_business_data():
    row = generate_v1_orders(1)[0]
    assert canonical_order_hash(row) != canonical_order_hash({**row, "quantity": 99})


def test_nulls_have_stable_explicit_representation():
    row = {**generate_v1_orders(1)[0], "customer_id": None}
    assert '"customer_id":null' in canonical_order_json(row)
    assert canonical_order_hash(row) == canonical_order_hash(dict(row))
