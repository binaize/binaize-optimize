from typing import List

from fastapi import Depends, HTTPException, status
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.security import OAuth2PasswordRequestForm
from jwt import PyJWTError

from optimization_platform.deployment.server_utils import *
from optimization_platform.src.service_layer.client import add_new_client, add_shopify_credentials_to_existing_client
from optimization_platform.src.service_layer.dashboard import get_session_count_per_variation_over_time, \
    get_visitor_count_per_variation_over_time, get_conversion_rate_per_variation_over_time, \
    get_conversion_rate_of_experiment
from optimization_platform.src.service_layer.event import register_event_for_client
from optimization_platform.src.service_layer.experiment import create_experiment_for_client_id, \
    get_experiments_for_client_id
from optimization_platform.src.service_layer.variation import create_variation_for_client_id_and_experiment_id, \
    get_variation_id_to_recommend
from utils.data_store.rds_data_store import RDSDataStore


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Binaize API",
        version="2.5.0",
        description="apis for the binaize service",
        routes=app.routes,
    )
    openapi_schema["info"]["x-logo"] = {
        "url": "https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png"
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app = FastAPI()
app.openapi = custom_openapi

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

rds_data_store = RDSDataStore(host=AWS_RDS_HOST, port=AWS_RDS_PORT,
                              dbname=AWS_RDS_DBNAME,
                              user=AWS_RDS_USER,
                              password=AWS_RDS_PASSWORD)


async def _get_current_client(token: str = Depends(oauth2_scheme)):
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


async def get_current_active_client(current_client: BaseClient = Depends(_get_current_client)):
    if current_client.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_client


@app.get("/", response_model=ResponseMessage)
async def home_page():
    response = ResponseMessage()
    response.message = "Binaize Optim and Nginx are up and running"
    response.status = status.HTTP_200_OK
    return response


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


@app.post("/sign_up", response_model=ResponseMessage)
async def sign_up_new_client(new_client: NewClient):
    user = get_client(rds_data_store, client_id=new_client.client_id)
    response = ResponseMessage()
    response.message = "Client_id {client_id} is already registered.".format(
        client_id=new_client.client_id)
    response.status = status.HTTP_409_CONFLICT
    if user is None:
        hashed_password = get_password_hash(new_client.password)
        add_new_client(data_store=rds_data_store, client_id=new_client.client_id, full_name=new_client.full_name,
                       company_name=new_client.company_name, hashed_password=hashed_password,
                       disabled=new_client.disabled)
        response.message = "Sign up for new client with client_id {client_id} is successful.".format(
            client_id=new_client.client_id)
        response.status = status.HTTP_200_OK

    return response


@app.post("/add_shopify_credentials", response_model=ResponseMessage)
async def add_shopify_credentials_to_logged_in_client(*, current_client: LoggedinClient = Depends(
    get_current_active_client),
                                                      shopify_credentials: ShopifyCredential):
    add_shopify_credentials_to_existing_client(data_store=rds_data_store, client_id=current_client.client_id,
                                               shopify_app_api_key=shopify_credentials.shopify_app_api_key,
                                               shopify_app_password=shopify_credentials.shopify_app_password,
                                               shopify_app_eg_url=shopify_credentials.shopify_app_eg_url,
                                               shopify_app_shared_secret=shopify_credentials.shopify_app_shared_secret)
    response = ResponseMessage()
    response.message = "Addition of Shopify Credentials for client_id {client_id} is successful.".format(
        client_id=current_client.client_id)
    response.status = status.HTTP_200_OK
    return response


@app.post("/add_experiment", response_model=Experiment)
async def add_experiment(*, current_client: ShopifyClient = Depends(get_current_active_client),
                         new_experiment: NewExperiment):
    experiment = create_experiment_for_client_id(data_store=rds_data_store, client_id=current_client.client_id,
                                                 experiment_name=new_experiment.experiment_name,
                                                 page_type=new_experiment.page_type,
                                                 experiment_type=new_experiment.experiment_type,
                                                 status=new_experiment.status,
                                                 created_on_timestamp=new_experiment.created_on_timestamp,
                                                 last_updated_on_timestamp=new_experiment.last_updated_on_timestamp)
    return experiment


@app.get("/list_experiments", response_model=List[Experiment])
async def list_experiments(*, current_client: ShopifyClient = Depends(get_current_active_client)):
    experiment_ids = get_experiments_for_client_id(data_store=rds_data_store, client_id=current_client.client_id)
    return experiment_ids


@app.post("/add_variation", response_model=Variation)
async def add_variation(*, current_client: ShopifyClient = Depends(get_current_active_client),
                        new_variation: NewVariation):
    variation = create_variation_for_client_id_and_experiment_id(data_store=rds_data_store,
                                                                 client_id=current_client.client_id,
                                                                 experiment_id=new_variation.experiment_id,
                                                                 variation_name=new_variation.variation_name,
                                                                 traffic_percentage=new_variation.traffic_percentage)
    return variation


@app.post("/get_variation_id_to_redirect", response_model=Variation)
async def get_variation_id_to_redirect(*, recommendation_request: RecommendationRequest):
    variation = get_variation_id_to_recommend(data_store=rds_data_store, client_id=recommendation_request.client_id,
                                              experiment_id=recommendation_request.experiment_id,
                                              session_id=recommendation_request.session_id)

    return variation


@app.post("/register_event", response_model=ResponseMessage)
async def register_event(*, event: Event):
    register_event_for_client(data_store=rds_data_store, client_id=event.client_id, experiment_id=event.experiment_id,
                              session_id=event.session_id, variation_id=event.variation_id, event_name=event.event_name)

    response = ResponseMessage()
    response.message = "Event registration for client_id {client_id} is successful.".format(
        client_id=event.client_id)
    response.status = status.HTTP_200_OK
    return response


@app.post("/get_session_count_for_dashboard", response_model=dict)
async def get_session_count_for_dashboard(*, current_client: ShopifyClient = Depends(get_current_active_client),
                                          experiment_id: str):
    result = get_session_count_per_variation_over_time(data_store=rds_data_store, client_id=current_client.client_id,
                                                       experiment_id=experiment_id)

    return result


@app.post("/get_visitor_count_for_dashboard", response_model=dict)
async def get_visitor_count_for_dashboard(*, current_client: ShopifyClient = Depends(get_current_active_client),
                                          experiment_id: str):
    result = get_visitor_count_per_variation_over_time(data_store=rds_data_store, client_id=current_client.client_id,
                                                       experiment_id=experiment_id)

    return result


@app.post("/get_conversion_rate_for_dashboard", response_model=dict)
async def get_conversion_rate_for_dashboard(*, current_client: ShopifyClient = Depends(get_current_active_client),
                                            experiment_id: str):
    result = get_conversion_rate_per_variation_over_time(data_store=rds_data_store, client_id=current_client.client_id,
                                                         experiment_id=experiment_id)

    return result


@app.post("/get_conversion_table_for_dashboard", response_model=List[dict])
async def get_conversion_table_for_dashboard(*, current_client: ShopifyClient = Depends(get_current_active_client),
                                             experiment_id: str):
    result = get_conversion_rate_of_experiment(data_store=rds_data_store, client_id=current_client.client_id,
                                               experiment_id=experiment_id)

    return result


@app.post("/get_experiment_summary", response_model=dict)
async def get_experiment_summary(*, current_client: ShopifyClient = Depends(get_current_active_client),
                                 experiment_id: str):
    result = dict()
    result["status"] = "Variation 1 is winning. It is 4% better than the others."
    result[
        "conclusion"] = "There is NOT enough evidence to conclude the experiment (It is NOT yet statistically significant)."
    result["recommendation"] = "Recommendation: CONTINUE the Experiment."
    return result
