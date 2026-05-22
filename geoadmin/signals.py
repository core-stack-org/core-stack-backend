# your_app/signals.py

from django.db.models.signals import post_save
from django.dispatch import receiver
from .models import TehsilSOI
import json
from pathlib import Path
from django.conf import settings
from .utils import activated_tehsils, transform_data

# Define cache file path


def generate_activated_locations_json_data():
    """Generate activated_locations_json and save to JSON file"""
    data_dir = Path(getattr(settings, "DATA_DIR", Path(settings.BASE_DIR) / "data"))
    activate_locations_file_path = (
        data_dir / "activated_locations" / "active_locations.json"
    )
    try:
        response_data = activated_tehsils()
        transformed_data = transform_data(data=response_data)
        activate_locations_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to JSON file
        with open(activate_locations_file_path, "w") as f:
            json.dump(transformed_data, f, indent=2)

        return transformed_data
    except Exception as e:
        print(f"Error activated_locations_json: {e}")
        raise


@receiver(post_save, sender=TehsilSOI)
def update_generate_activated_locations_json_data(sender, instance, created, **kwargs):
    """Only regenerate data if active_status field was modified"""
    try:
        if instance.active_status is not None:
            generate_activated_locations_json_data()
            print(
                f"Activated_locations_json regenerated after Block {instance.id} was {'created' if created else 'updated'}"
            )
    except Exception as e:
        print(f"Failed to update activated_locations_json data: {e}")
