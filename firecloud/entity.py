#! /usr/bin/env python

import firecloud.api as fapi
import json

class Entity(object):
    """
    Class reperesentation of a FireCloud Entity
    """
    ENTITY_TYPES = { "participant", "participant_set",
                     "sample",      "sample_set",
                     "pair",        "pair_set"
                   }

    def __init__(self, etype, entity_id, attrs=dict()):
        if etype not in Entity.ENTITY_TYPES:
            raise ValueError("Invalid entity type: " + etype)
        self.entity_id = entity_id
        self.etype = etype
        self.attrs = attrs
    
    def get_attribute(self, attr):
        return self.attrs.get(attr, None)

    def set_attribute(self, attr, value):
        self.attrs[attr] = value
        return value

    @staticmethod
    def create_payload(entities):
        """
        Create a tsv payload that can be encoded in an importEntities API call
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
        with open(f, 'w') as out:
            out.write(create_payload(entities))
