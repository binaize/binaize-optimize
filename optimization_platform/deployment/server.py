from fastapi import Depends, HTTPException, status
from fastapi import FastAPI
from fastapi.security import OAuth2PasswordRequestForm
from jwt import PyJWTError

from optimization_platform.deployment.app_utils import *
from optimization_platform.src.service_layer.serve import add_new_client, add_shopify_credentials_to_existing_client
from utils.data_store.rds_data_store import RDSDataStore

rds_data_store = RDSDataStore(host=AWS_RDS_HOST, port=AWS_RDS_PORT,
                              dbname=AWS_RDS_DBNAME,
                              user=AWS_RDS_USER,
                              password=AWS_RDS_PASSWORD)

app = FastAPI()


async def get_current_client(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        client_id: str = payload.get("sub")
        if client_id is None:
            raise credentials_exception
        token_data = TokenData(client_id=client_id)
    except PyJWTError:
        raise credentials_exception
    user = get_client(rds_data_store, client_id=token_data.client_id)
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_client(current_client: Client = Depends(get_current_client)):
    if current_client.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_client


@app.post("/token", response_model=Token)
async def login_and_get_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_client(rds_data_store, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.client_id}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/clients/me/", response_model=Client)
async def read_current_client(current_client: Client = Depends(get_current_active_client)):
    return current_client


@app.post("/sign_up", response_model=SuccessMessage)
async def sign_up_new_client(client: NewClient):
    hashed_password = get_password_hash(client.password)
    add_new_client(data_store=rds_data_store, client_id=client.client_id, full_name=client.full_name,
                   company_name=client.company_name, hashed_password=hashed_password, disabled=client.disabled)
    return SuccessMessage()


# send multiple post parameters along with access token
@app.get("/test", response_model=str)
async def read_current_client(*, current_client: Client = Depends(get_current_active_client), s: str):
    return current_client.client_id + s


@app.post("/add_shopify_credentials", response_model=dict)
async def add_shopify_credentials(*, current_client: Client = Depends(get_current_active_client),
                                  shopify_credentials: ShopifyCredential):
    add_shopify_credentials_to_existing_client(data_store=rds_data_store, client_id=current_client.client_id,
                                               shopify_app_api_key=shopify_credentials.shopify_app_api_key,
                                               shopify_app_password=shopify_credentials.shopify_app_password,
                                               shopify_app_eg_url=shopify_credentials.shopify_app_eg_url,
                                               shopify_app_shared_secret=shopify_credentials.shopify_app_shared_secret)
    d = {"status_code": status.HTTP_200_OK}
    return d
