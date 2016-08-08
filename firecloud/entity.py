import json

import firecloud.api as fapi


class Entity(object):
    """A FireCloud Entity

    Attributes:
        etype (str): Enity type, e.g. "sample". Must be one of
            Entity.ENTITY_TYPES
        entity_id (str): Unique id of this entity. Becomes the entity's
            name in FireCloud.
        attrs (dict): Dictionary of attributes and their values
    """
    ENTITY_TYPES = { "participant", "participant_set",
                     "sample",      "sample_set",
                     "pair",        "pair_set"
                   }

    def __init__(self, etype, entity_id, attrs=dict()):
        """Create Entity."""

        if etype not in Entity.ENTITY_TYPES:
            raise ValueError("Invalid entity type: " + etype)
        self.entity_id = entity_id
        self.etype = etype
        self.attrs = attrs

    def get_attribute(self, attr):
        """Return attribute value."""
        return self.attrs.get(attr, None)

    def set_attribute(self, attr, value):
        """Set and return attribute value."""
        self.attrs[attr] = value
        return value

    @staticmethod
    def create_payload(entities):
        """Create a tsv payload describing entities.

        A TSV payload consists of 1 header row describing entity type
        and attribute names. Each subsequent line is an entity_id followed
        by attribute values separated by the tab "\\t" character. This
        payload can be uploaded to the workspace via
        firecloud.api.upload_entities()
        """
        #First check that all entities are of the same type
        types = {e.etype for e in entities}
        if len(types) != 1:
            raise ValueError("Can't create payload with " +
                             str(len(types)) + " types")

        all_attrs = set()
        for e in entities:
            all_attrs.update(set(e.attrs.keys()))

        #Write a header line
        all_attrs = list(all_attrs)
        header = "entity:" + entities[0].etype + "_id"
        payload = '\t'.join([header] + all_attrs) + '\n'

        for e in entities:
            line = e.entity_id
            for a in all_attrs:
                line += '\t' + e.attrs.get(a, "")
            payload += line + '\n'

        return payload

    @staticmethod
    def create_loadfile(entities, f):
        """Create payload and save to file."""
        with open(f, 'w') as out:
            out.write(create_payload(entities))
