import enum


class SegmentChangeType(enum.Enum):
    new = "new"
    active = "active"
    removed = "removed"
