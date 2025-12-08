from django.db import models
import uuid
from geoadmin.models import State, District, Block
from projects.models import Project
from users.models import User
from bot_interface.models import Bot


# Create your models here.
class LocationLevel(models.TextChoices):
    STATE = "state", "State"
    DISTRICT = "district", "District"
    BLOCK = "block", "Block"


class Location(models.Model):
    level = models.CharField(max_length=10, choices=LocationLevel.choices)
    state = models.ForeignKey(State, on_delete=models.CASCADE)
    district = models.ForeignKey(
        District, on_delete=models.CASCADE, null=True, blank=True
    )
    block = models.ForeignKey(Block, on_delete=models.CASCADE, null=True, blank=True)

    class Meta:
        unique_together = ("level", "state", "district", "block")

    def __str__(self):
        parts = []

        if self.state:
            parts.append(self.state.state_name)
        if self.district:
            parts.append(self.district.district_name)
        if self.block:
            parts.append(self.block.block_name)

        label = " / ".join(parts) if parts else "Unknown location"
        return f"[{self.level.upper()}] {label}"


class Community(models.Model):
    id = models.AutoField(primary_key=True)
    project = models.ForeignKey(Project, null=True, on_delete=models.CASCADE)
    bot = models.ForeignKey(Bot, null=True, on_delete=models.SET_NULL)
    locations = models.ManyToManyField(Location, related_name="communities")

    def __str__(self):
        return self.project.name


class Community_user_mapping(models.Model):
    id = models.AutoField(primary_key=True)
    community = models.ForeignKey(Community, on_delete=models.CASCADE)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    is_last_accessed_community = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)


class Media_type(models.TextChoices):
    IMAGE = "IMAGE", "IMAGE"
    AUDIO = "AUDIO", "AUDIO"
    VIDEO = "VIDEO", "VIDEO"
    DOC = "DOC", "DOC"


class Media_source(models.TextChoices):
    BOT = "BOT", "BOT"
    IVR = "IVR", "IVR"


class Media(models.Model):
    id = models.AutoField(primary_key=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    media_type = models.CharField(max_length=255, choices=Media_type.choices)
    media_path = models.CharField(max_length=255)
    source = models.CharField(max_length=255, choices=Media_source.choices)
    bot = models.ForeignKey(Bot, null=True, on_delete=models.SET_NULL)
    created_at = models.DateTimeField(auto_now_add=True)


class Item_category(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name


class Item_type(models.TextChoices):
    STORY = "STORY", "STORY"
    GRIEVANCE = "GRIEVANCE", "GRIEVANCE"
    ASSET_DEMAND = "ASSET_DEMAND", "ASSET_DEMAND"
    CONTENT = "CONTENT", "CONTENT"


class Item_state(models.TextChoices):
    UNMODERATED = "UNMODERATED", "UNMODERATED"
    PUBLISHED = "PUBLISHED", "PUBLISHED"
    REJECTED = "REJECTED", "REJECTED"
    INPROGRESS = "INPROGRESS", "INPROGRESS"
    RESOLVED = "RESOLVED", "RESOLVED"
    ACCEPTED_STAGE_1 = "ACCEPTED_STAGE_1", "ACCEPTED_STAGE_1"
    REJECTED_STAGE_1 = "REJECTED_STAGE_1", "REJECTED_STAGE_1"


ITEM_TYPE_STATE_MAP = {
    Item_type.CONTENT: [
        Item_state.UNMODERATED,
        Item_state.PUBLISHED,
        Item_state.REJECTED,
    ],
    Item_type.GRIEVANCE: [
        Item_state.UNMODERATED,
        Item_state.INPROGRESS,
        Item_state.RESOLVED,
        Item_state.REJECTED,
    ],
    Item_type.ASSET_DEMAND: [
        Item_state.UNMODERATED,
        Item_state.ACCEPTED_STAGE_1,
        Item_state.REJECTED_STAGE_1,
        Item_state.INPROGRESS,
        Item_state.RESOLVED,
    ],
    Item_type.STORY: [
        Item_state.UNMODERATED,
        Item_state.PUBLISHED,
        Item_state.REJECTED,
    ],
}


class Item(models.Model):
    id = models.AutoField(primary_key=True)
    title = models.CharField(max_length=255)
    transcript = models.TextField(blank=True, null=True)
    category = models.ForeignKey(
        Item_category, blank=True, null=True, on_delete=models.SET_NULL
    )
    rating = models.PositiveSmallIntegerField(default=0)
    media = models.ManyToManyField(Media, blank=True)
    item_type = models.CharField(max_length=255, choices=Item_type.choices)
    coordinates = models.TextField(blank=True, null=True)
    state = models.CharField(max_length=255, choices=Item_state.choices)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    community = models.ForeignKey(Community, on_delete=models.CASCADE)
    misc = models.JSONField(blank=True, null=True, default=dict)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
