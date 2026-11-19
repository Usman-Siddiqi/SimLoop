"""A placeholder model: Tortoise.init() needs at least one models module."""

from tortoise import fields
from tortoise.models import Model


class Item(Model):
    id = fields.IntField(primary_key=True)
