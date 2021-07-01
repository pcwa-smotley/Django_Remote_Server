from django import template

register = template.Library()

@register.filter
def custom_string_rep(value):
    new_val = value.replace("_"," ")
    new_val = new_val.replace("afterbay", "abay")
    return new_val.upper()