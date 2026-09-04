"""Feast entities shared by offline training and online inference."""
from feast import Entity
from feast.value_type import ValueType

asset = Entity(name="asset", join_keys=["asset"], value_type=ValueType.STRING)
frequency = Entity(name="frequency", join_keys=["frequency"], value_type=ValueType.STRING)
