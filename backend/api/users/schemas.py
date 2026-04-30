from typing import List, Optional

from pydantic import BaseModel, validator


class CBUsers(BaseModel):
    id: int
    relation_id: Optional[int] = None
    phone_number: Optional[str]
    external_id: Optional[str]
    first_name: str
    last_name: Optional[str]
    username: Optional[str]
    status: bool
    photo: str
    is_admin: bool
    created_at: int
    updated_at: int
    tags: Optional[List[str]]
    timezone: Optional[str]
    payment_past_edit_days: Optional[int]
    shift_work_enabled: Optional[bool]

    class Config:
        orm_mode = True

    @validator("tags")
    def validate_tags(cls, tag_list):
        return tag_list or []


class CBUsersShort(BaseModel):
    id: int
    external_id: Optional[str]
    first_name: str
    last_name: Optional[str]
    status: bool
    shift_work_enabled: Optional[bool] = False

    class Config:
        orm_mode = True


class CBUsersListShort(BaseModel):
    result: List[CBUsersShort]
    count: int

    class Config:
        orm_mode = True


class CBUsersList(BaseModel):
    result: List[CBUsers]
    count: int

    class Config:
        orm_mode = True


class PermissionItem(BaseModel):
    section: str
    can_view: bool = True
    can_edit: bool = False
    paybox_id: Optional[int] = None


class UserPermissionUpdate(BaseModel):
    user_id: int
    permissions: List[PermissionItem]


class UserPermissionResponse(BaseModel):
    status: str = "success"
    message: str = "Права пользователя обновлены"


class UserPermissionsList(BaseModel):
    section: str
    can_view: bool
    can_edit: bool
    paybox_id: Optional[int] = None
    paybox_name: Optional[str] = None


class UserPermissionsResult(BaseModel):
    is_admin: Optional[bool] = None
    user_id: int
    first_name: str
    last_name: Optional[str]
    username: Optional[str]
    permissions: List[UserPermissionsList]


class UserShiftSettings(BaseModel):
    shift_work_enabled: bool


class UserShiftSettingsResponse(BaseModel):
    success: bool
    message: str
    shift_work_enabled: bool


class UserWithShiftInfo(BaseModel):
    id: int
    first_name: str
    last_name: Optional[str] = ""
    username: Optional[str] = ""
    photo: Optional[str] = ""
    shift_work_enabled: bool = False
    current_shift_status: Optional[str] = None
    shift_duration_minutes: Optional[int] = None


class CBUsersListShortWithShifts(BaseModel):
    result: List[UserWithShiftInfo]
    count: int
    on_shift_total: int = 0  # Общее количество на сменах


class ShiftStatistics(BaseModel):
    on_shift_count: int = 0
    on_break_count: int = 0
    total_active: int = 0
    shift_enabled_users: int = 0
