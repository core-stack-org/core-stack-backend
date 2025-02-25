from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field

class OrganizationBase(BaseModel):
    name: str
    description: str | None = None

class OrganizationCreate(OrganizationBase):
    pass

class OrganizationRead(OrganizationBase):
    id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True