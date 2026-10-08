from django.db.models.signals import pre_save
from django.dispatch import receiver

from .models import (
    ODK_waterbody,
    ODK_well,
)

# Maps a model to the (raw data field, modified-data field, {source_key: preserved_key})
# it needs original values preserved for, before any mapping.py normalization
# (e.g. classify_demand_type) can overwrite them downstream.
_PRESERVED_FIELDS = {
    ODK_waterbody: (
        "data_waterbody",
        "modified_data_waterbody",
        {"select_one_owns": "demand_type"},
    ),
    ODK_well: (
        "data_well",
        "modified_data_well",
        {"select_one_owns": "demand_type"},
    ),
}


def _preserve_original_values(instance, data_field, modified_data_field, field_map):
    """
    Snapshot the original (pre-normalization) value of each source key in
    field_map into modified_data_field, keyed by its preserved_key. Each
    value is preserved only once and never overwritten on later saves.
    """
    data = getattr(instance, data_field, None) or {}
    modified_data = getattr(instance, modified_data_field, None) or {}

    changed = False
    for source_key, preserved_key in field_map.items():
        if preserved_key in modified_data:
            continue
        original_value = data.get(source_key)
        if original_value is not None:
            modified_data[preserved_key] = original_value
            changed = True

    if changed:
        setattr(instance, modified_data_field, modified_data)


@receiver(pre_save, sender=ODK_waterbody)
@receiver(pre_save, sender=ODK_well)
def preserve_original_data(sender, instance, **kwargs):
    data_field, modified_data_field, field_map = _PRESERVED_FIELDS[sender]
    _preserve_original_values(instance, data_field, modified_data_field, field_map)
