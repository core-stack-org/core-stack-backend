from django.db import models
import uuid
from geoadmin.models import State, District, Block
from projects.models import Project
from users.models import User


# Create your models here.
class Community(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    project = models.ForeignKey(Project, on_delete=models.CASCADE)
    # bot = models.ForeignKey(Bot, on_delete=models.SET_NULL)

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
    # bot = models.ForeignKey(Bot, on_delete=models.SET_NULL)
    created_at = models.DateTimeField(auto_now_add=True)


class Item_category(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name


class Item_type(models.TextChoices):
    STORY = "STORY", "STORY"
    GRIEVANCE = "GRIEVANCE", "GRIEVANCE"
    WORK_DEMAND = "WORK_DEMAND", "WORK_DEMAND"
    CONTENT = "CONTENT", "CONTENT"


class Item_state(models.TextChoices):
    PUB = "PUB", "PUB"
    ARC = "ARC", "ARC"
    UNM = "UNM", "UNM"
    REJ = "REJ", "REJ"


class Item(models.Model):
    id = models.AutoField(primary_key=True)
    title = models.CharField(max_length=255)
    transcript = models.TextField(blank=True, null=True)
    category = models.ForeignKey(Item_category, blank=True, null=True, on_delete=models.SET_NULL)
    rating = models.PositiveSmallIntegerField(default=0)
    media = models.ManyToManyField(Media, blank=True)
    item_type = models.CharField(max_length=255, choices=Item_type.choices)
    coordinates = models.TextField(blank=True, null=True)
    state = models.CharField(max_length=255, choices=Item_state.choices)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    community = models.ForeignKey(Community, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)