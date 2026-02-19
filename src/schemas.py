import pandera as pa
from pandera import Column, Check


VALID_BREWERY_TYPES = [
    """
    Breweries types accordingly to api documentation
    """
    'micro', 'nano', 'regional', 'brewpub', 'large', 
    'planning', 'bar', 'contract', 'proprietary', 'closed'
]


brewery_silver_schema = pa.DataFrameSchema({
    """
    Schema of silver transformations, included limit range for latitude and longitude
    """
    'id': Column(str, unique=True, nullable=False),
    'name': Column(str, nullable=False),
    'brewery_type': Column(str, Check.isin(VALID_BREWERY_TYPES)),
    'latitude': Column(float, Check.between(-90, 90), nullable=True),
    'longitude': Column(float, Check.between(-180, 180), nullable=True),
    'state_province': Column(str, nullable=False)
})