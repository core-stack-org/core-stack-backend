from .models import Media_type, Community, Location, LocationLevel
from geoadmin.models import State, District, Block
from django.db.models import Q
from django.utils.timezone import now, localtime



def get_media_type(param):
    param = param.lower()
    if "image" in param:
        return Media_type.IMAGE.value
    elif "audio" in param:
        return Media_type.AUDIO.value
    elif "video" in param:
        return Media_type.VIDEO.value
    elif "doc" in param:
        return Media_type.DOC.value
    return ""


def get_community_summary_data(community_id):
    community = Community.objects.get(id=community_id)
    return {"community_id": community_id,
            "name": str(community.project.name),
            "description": str(community.project.description),
            "organization": str(community.project.organization)}


def get_communities(state_name, district_name, block_name):
    state_obj = State.objects.filter(state_name__iexact=state_name).first()
    district_obj = District.objects.filter(district_name__iexact=district_name).first()
    block_obj = Block.objects.filter(block_name__iexact=block_name).first()

    if state_obj and not district_obj and not block_obj:
        communities = Community.objects.filter(locations__state=state_obj)
    elif district_obj and not state_obj and not block_obj:
        communities = Community.objects.filter(locations__district=district_obj)
    elif block_obj and not state_obj and not district_obj:
        communities = Community.objects.filter(locations__block=block_obj)
    elif state_obj and district_obj and not block_obj:
        communities1 = Community.objects.filter(locations__state=state_obj, locations__district=district_obj).distinct()
        communities2 = Community.objects.filter(locations__state=state_obj, locations__district__isnull=True, locations__block__isnull=True).distinct()
        communities = communities1 | communities2
    elif state_obj and block_obj and not district_obj:
        communities = Community.objects.filter(locations__state=state_obj)
    elif district_obj and block_obj and not state_obj:
        communities = Community.objects.filter(locations__district=district_obj)
    elif block_obj and district_obj and state_obj:
        communities1 = Community.objects.filter(locations__state=state_obj, locations__district=district_obj).distinct()
        communities2 = Community.objects.filter(locations__state=state_obj, locations__district__isnull=True, locations__block__isnull=True).distinct()
        communities = communities1 | communities2
    else:
        communities = Community.objects.all()

    data = [get_community_summary_data(c.id) for c in communities]
    return data


def create_community_for_project(project):
    if project.block_id:
        level = LocationLevel.BLOCK
    elif project.district_id:
        level = LocationLevel.DISTRICT
    elif project.state_id:
        level = LocationLevel.STATE
    else:
        raise ValueError("Project has no location fields set")

    location, created = Location.objects.get_or_create(
        level=level,
        state_id=project.state_id,
        district_id=project.district_id,
        block_id=project.block_id,
    )

    community = Community.objects.create(project=project)
    community.locations.add(location)
    return community


def generate_item_title(item_type):
    display_type = item_type.replace("_", " ").title()
    current_time = localtime(now())
    date_str = current_time.strftime("%d %b %Y")
    time_str = current_time.strftime("%I:%M %p").lstrip("0")
    return f"{display_type} - {date_str}, {time_str}"