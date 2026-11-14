import copy

import pytest
import yaml

from lakewatch.planner.policy import Policy, PolicyError


def _base(policy):
    return copy.deepcopy(policy.to_dict())


def test_policy_exceptions_and_hash(policy):
    orders = policy.settings("orders")
    events = policy.settings("events")
    assert orders.max_files_per_partition == 8
    assert orders.compact_max_inputs == 8
    assert events.max_files_listed == 6
    assert policy.settings("customers").enabled is True
    clone = Policy.from_dict(_base(policy))
    assert clone.policy_hash == policy.policy_hash
    assert clone.as_of_iso == "2025-03-10T00:00:00Z"


def test_missing_unknown_and_invalid_fields_are_rejected(policy):
    missing = _base(policy)
    del missing["max_actions"]
    with pytest.raises(PolicyError, match="missing policy field"):
        Policy.from_dict(missing)

    unknown = _base(policy)
    unknown["mystery"] = 1
    with pytest.raises(PolicyError, match="unknown policy field"):
        Policy.from_dict(unknown)

    bad_retention = _base(policy)
    bad_retention["retention"]["unexpected"] = True
    with pytest.raises(PolicyError, match="unknown retention field"):
        Policy.from_dict(bad_retention)

    bad_value = _base(policy)
    bad_value["small_file_bytes"] = 0
    with pytest.raises(PolicyError, match="small_file_bytes"):
        Policy.from_dict(bad_value)

    bad_bool = _base(policy)
    bad_bool["compact_small_files"] = "yes"
    with pytest.raises(PolicyError, match="compact_small_files"):
        Policy.from_dict(bad_bool)


def test_table_exception_validation(policy):
    payload = _base(policy)
    payload["tables"]["orders"]["nope"] = 3
    with pytest.raises(PolicyError, match="unknown field"):
        Policy.from_dict(payload)

    payload = _base(policy)
    payload["tables"]["orders"]["retention"] = {"keep_days": -1}
    with pytest.raises(PolicyError, match="keep_days"):
        Policy.from_dict(payload)

    payload = _base(policy)
    payload["tables"]["orders"]["compact_small_files"] = False
    parsed = Policy.from_dict(payload)
    assert parsed.settings("orders").compact_small_files is False
    assert parsed.settings("events").compact_small_files is True


def test_policy_yaml_loader(tmp_path, policy):
    path = tmp_path / "policy.yaml"
    path.write_text(yaml.safe_dump(_base(policy)), encoding="utf-8")
    loaded = Policy.from_yaml(path)
    assert loaded.policy_hash == policy.policy_hash
    with pytest.raises(PolicyError):
        Policy.from_yaml(tmp_path / "missing.yaml")


def _with_cluster(policy, columns, table="orders"):
    payload = _base(policy)
    payload["tables"].setdefault(table, {})["cluster"] = {"columns": columns}
    return payload


def test_cluster_contract_is_parsed_and_hashed(policy):
    columns = [
        {"name": "quantity", "direction": "desc", "nulls": "first"},
        {"name": "amount", "direction": "asc", "nulls": "last"},
    ]
    parsed = Policy.from_dict(_with_cluster(policy, columns))
    cluster = parsed.settings("orders").cluster
    assert [item.as_dict() for item in cluster] == columns
    assert parsed.settings("events").cluster is None
    assert parsed.policy_hash != policy.policy_hash
    clone = Policy.from_dict(parsed.to_dict())
    assert clone.policy_hash == parsed.policy_hash
    assert [item.as_dict() for item in clone.settings("orders").cluster] == columns


def test_cluster_contract_rejects_invalid_values(policy):
    bad_direction = _with_cluster(
        policy, [{"name": "quantity", "direction": "up", "nulls": "first"}]
    )
    with pytest.raises(PolicyError, match="direction"):
        Policy.from_dict(bad_direction)

    bad_nulls = _with_cluster(
        policy, [{"name": "quantity", "direction": "asc", "nulls": "middle"}]
    )
    with pytest.raises(PolicyError, match="nulls"):
        Policy.from_dict(bad_nulls)

    missing_nulls = _with_cluster(policy, [{"name": "quantity", "direction": "asc"}])
    with pytest.raises(PolicyError, match="missing"):
        Policy.from_dict(missing_nulls)

    duplicate = _with_cluster(
        policy,
        [
            {"name": "quantity", "direction": "asc", "nulls": "first"},
            {"name": "quantity", "direction": "desc", "nulls": "last"},
        ],
    )
    with pytest.raises(PolicyError, match="more than once"):
        Policy.from_dict(duplicate)

    empty = _with_cluster(policy, [])
    with pytest.raises(PolicyError, match="non-empty"):
        Policy.from_dict(empty)

    payload = _base(policy)
    payload["tables"]["orders"]["cluster"] = {"columns": [{"name": "q"}]}
    with pytest.raises(PolicyError, match="missing"):
        Policy.from_dict(payload)

    payload = _base(policy)
    payload["tables"]["orders"]["cluster"] = "quantity"
    with pytest.raises(PolicyError, match="mapping"):
        Policy.from_dict(payload)

    payload = _with_cluster(
        policy, [{"name": "q", "direction": "asc", "nulls": "first", "extra": 1}]
    )
    with pytest.raises(PolicyError, match="unknown field"):
        Policy.from_dict(payload)
