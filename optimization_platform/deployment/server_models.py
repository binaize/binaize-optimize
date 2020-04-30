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


class SuccessMessage(BaseModel):
    message: str = "yayyy!!!!"
