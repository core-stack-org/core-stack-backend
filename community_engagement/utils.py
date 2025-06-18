from .models import Media_type, Community


def get_media_type(param):
    if param=="images":
        return Media_type.IMAGE.value
    elif param=="audios":
        return Media_type.AUDIO.value
    elif param=="videos":
        return Media_type.VIDEO.value
    elif params=="docs":
        return Media_type.DOC.value
    return ""


def get_community_summary_data(community_id):
    community = Community.objects.get(id=community_id)
    return {"community_id": community_id,
            "name": str(community.project.name),
            "description": str(community.project.description),
            "organization": str(community.project.organization)}

