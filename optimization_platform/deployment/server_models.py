from pydantic import BaseModel


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    client_id: str = None


class Client(BaseModel):
    client_id: str
    company_name: str
    full_name: str
    disabled: bool


class ClientInDB(Client):
    hashed_password: str


class NewClient(Client):
    password: str


class ShopifyCredential(BaseModel):
    shopify_app_api_key: str = None
    shopify_app_password: str = None
    shopify_app_eg_url: str = None
    shopify_app_shared_secret: str = None


class ResponseMessage(BaseModel):
    status: str = None
    message: str = None
