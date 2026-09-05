"""Feast entities shared by historical retrieval and online inference."""
from feast import Entity
from feast.value_type import ValueType

asset = Entity(name="asset", join_keys=["asset"], value_type=ValueType.STRING)
