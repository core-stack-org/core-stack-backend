from django.db.models.signals import pre_save
from django.dispatch import receiver

from .models import (
    ODK_waterbody,
    ODK_well,
)


def preserve_unmodified_demand_type(
    instance,
    data_field,
    unmodified_data_field,
    source_field,
):
    """
    Preserve only the original value of the field that is modified
    by classify_demand_type().
    """

    data = getattr(instance, data_field, None) or {}
    unmodified_data = getattr(instance, unmodified_data_field, None) or {}

    # Original ODK value
    original_value = data.get(source_field)

    # Preserve it only once.
    # Never overwrite the already preserved original value.
    if original_value is not None and "demand_type" not in unmodified_data:
        unmodified_data["demand_type"] = original_value

        setattr(
            instance,
            unmodified_data_field,
            unmodified_data,
        )


@receiver(pre_save, sender=ODK_waterbody)
def preserve_waterbody_unmodified_demand_type(
    sender,
    instance,
    **kwargs,
):
    preserve_unmodified_demand_type(
        instance=instance,
        data_field="data_waterbody",
        unmodified_data_field="unmodified_data_waterbody",
        source_field="select_one_owns",
    )


@receiver(pre_save, sender=ODK_well)
def preserve_well_unmodified_demand_type(
    sender,
    instance,
    **kwargs,
):
    preserve_unmodified_demand_type(
        instance=instance,
        data_field="data_well",
        unmodified_data_field="unmodified_data_well",
        source_field="select_one_owns",
    )