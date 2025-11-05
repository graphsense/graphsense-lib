class ObfuscatableTag:
    label: str
    source: str
    tagpack_uri: str
    actor: str
    tagpack_is_public: bool


def obfuscate_tag(t: ObfuscatableTag) -> ObfuscatableTag:
    t.label = ""
    t.source = ""
    t.tagpack_uri = ""
    t.actor = ""
    return t


def obfuscate_tag_cond(t: ObfuscatableTag, condition: bool) -> ObfuscatableTag:
    if condition:
        return obfuscate_tag(t)
    else:
        return t


def obfuscate_tag_if_not_public(t: ObfuscatableTag) -> ObfuscatableTag:
    if not t.tagpack_is_public:
        return obfuscate_tag(t)
    else:
        return t


def obfuscate_entity_actor(entity):
    if not entity:
        return
    if not entity.actors:
        return
    if not entity.best_address_tag:
        return
    if entity.best_address_tag.tagpack_is_public:
        return
    for actor in entity.actors:
        actor.id = ""
        actor.label = ""
