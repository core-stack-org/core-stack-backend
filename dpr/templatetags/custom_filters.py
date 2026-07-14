from django import template

register = template.Library()


@register.filter
def format_text(value):
    if not value:
        return "NA"

    return str(value).replace("_", " ")
