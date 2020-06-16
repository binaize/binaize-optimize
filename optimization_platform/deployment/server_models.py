from pydantic import BaseModel


class Token(BaseModel):
    access_token: str
    token_type: str


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


class BaseExperiment(BaseModel):
    experiment_name: str
    page_type: str
    experiment_type: str
    status: str


class Experiment(BaseExperiment):
    client_id: str
    experiment_id: str
    creation_time: str
    last_updation_time: str


class NewVariation(BaseModel):
    experiment_id: str
    variation_name: str
    traffic_percentage: int


class Variation(BaseModel):
    variation_id: str
    client_id: str
    experiment_id: str


class Event(BaseModel):
    client_id: str
    experiment_id: str
    variation_id: str
    session_id: str
    event_name: str


class Visit(BaseModel):
    client_id: str
    session_id: str
    event_name: str
    url: str


class Cookie(BaseModel):
    client_id: str
    session_id: str
    shopify_x: str
    cart_token: str