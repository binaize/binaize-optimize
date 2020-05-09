from pydantic import BaseModel


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    client_id: str = None


class BaseClient(BaseModel):
    client_id: str
    company_name: str
    full_name: str
    disabled: bool


class NewClient(BaseClient):
    password: str


class ShopifyCredential(BaseModel):
    shopify_app_api_key: str = None
    shopify_app_password: str = None
    shopify_app_eg_url: str = None
    shopify_app_shared_secret: str = None


class LoggedinClient(BaseClient):
    hashed_password: str


class ShopifyClient(LoggedinClient, ShopifyCredential):
    pass


class ResponseMessage(BaseModel):
    status: str = None
    message: str = None


class NewExperiment(BaseModel):
    experiment_name: str
    page_type: str
    experiment_type: str


class Experiment(NewExperiment):
    client_id: str
    experiment_id: str


class NewVariation(BaseModel):
    experiment_id: str
    variation_name: str = None
    traffic_percentage: int = None


class Variation(NewVariation):
    variation_id: str
    client_id: str
    s3_bucket_name: str = None
    s3_html_location: str = None


class RecommendationRequest(BaseModel):
    client_id: str
    experiment_id: str
    session_id: str


class Event(BaseModel):
    client_id: str
    experiment_id: str
    variation_id: str
    session_id: str
    event_name: str
