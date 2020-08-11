from pydantic import BaseModel


class Token(BaseModel):
    access_token: str
    token_type: str


class BinaizeShop(BaseModel):
    shop_id: str
    shop_domain: str
    shop_owner: str
    email_id: str
    shopify_access_token: str
    city: str
    country: str
    province: str
    disabled: bool
    hashed_password: str
    creation_time: str
    timezone: str


class ShopifyShop(BaseModel):
    shop_id: str
    shopify_access_token: str


class Shop(BaseModel):
    shop_id: str
    shop_domain: str
    shop_owner: str
    email_id: str
    city: str
    country: str
    province: str
    disabled: bool
    creation_time: str
    timezone: str


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
    cart_token: str


class Visitor(BaseModel):
    client_id: str
    session_id: str
    ip: str
    city: str
    region: str
    country: str
    lat: str
    long: str
    timezone: str
    browser: str
    os: str
    device: str
    fingerprint: str
