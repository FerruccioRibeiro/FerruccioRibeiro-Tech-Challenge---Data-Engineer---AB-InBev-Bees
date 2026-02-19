from pydantic import BaseModel, Field
from typing import Optional

class BreweryModel(BaseModel):
    """
    API model accordingly to api documentation
    """
    id: str = Field(..., description='Unique breweries ID')
    name: str
    brewery_type: str
    address_1: Optional[str] = None
    address_2: Optional[str] = None
    address_3: Optional[str] = None
    city: str
    state_province: str
    postal_code: str
    country: str
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)
    phone: Optional[str] = None
    website_url: Optional[str] = None
    state: str
    street: Optional[str] = None
