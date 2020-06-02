from datetime import datetime, timedelta
from typing import List

import jwt
from fastapi import Depends, HTTPException, status
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.security import OAuth2PasswordBearer
from fastapi.security import OAuth2PasswordRequestForm
from passlib.context import CryptContext

from config import *
from optimization_platform.deployment.server_models import *
from optimization_platform.src.agents.client_agent import ClientAgent
from optimization_platform.src.agents.dashboard_agent import DashboardAgent
from optimization_platform.src.agents.event_agent import EventAgent
from optimization_platform.src.agents.experiment_agent import ExperimentAgent
from optimization_platform.src.agents.variation_agent import VariationAgent
from optimization_platform.src.agents.visits_agent import VisitAgent
from utils.data_store.rds_data_store import RDSDataStore
from utils.logger.pylogger import get_logger
from utils.date_utils import DateUtils

logger = get_logger("server", "INFO")


def custom_openapi():
    openapi_schema = get_openapi(
        title="Binaize API",
        version="2.5.0",
        description="apis for the binaize service",
        routes=app.routes,
    )
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

app.rds_data_store = RDSDataStore(host=AWS_RDS_HOST, port=AWS_RDS_PORT,
                                  dbname=AWS_RDS_DBNAME,
                                  user=AWS_RDS_USER,
                                  password=AWS_RDS_PASSWORD)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def get_client(data_store, client_id: str):
    client_details = ClientAgent.get_client_details_for_client_id(data_store=data_store, client_id=client_id)
    if client_details is not None:
        return ShopifyClient(**client_details)


def authenticate_client(data_store, client_id: str, password: str):
    user = get_client(data_store, client_id)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


def create_access_token(*, data: dict, expires_delta: timedelta):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def _get_current_client(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        client_id = payload.get("sub")
        user = get_client(app.rds_data_store, client_id=client_id)
        if user is None:
            raise credentials_exception
        return user
    except Exception:
        raise credentials_exception


async def get_current_active_client(current_client: BaseClient = Depends(_get_current_client)):
    if current_client.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_client


@app.get("/", response_model=dict)
async def home_page():
    import time
    t = time.time()
    response = dict()
    response["message"] = custom_openapi()["info"]
    response["status"] = status.HTTP_200_OK
    logger.info("prod info message")
    logger.info(time.time() - t)
    return response


@app.post("/sign_up", response_model=ResponseMessage)
async def sign_up_new_client(new_client: NewClient):
    user = get_client(app.rds_data_store, client_id=new_client.client_id)
    response = ResponseMessage()
    response.message = "Client_id {client_id} is already registered.".format(
        client_id=new_client.client_id)
    response.status = status.HTTP_409_CONFLICT
    if user is None:
        hashed_password = get_password_hash(new_client.password)
        ClientAgent.add_new_client(data_store=app.rds_data_store, client_id=new_client.client_id,
                                   full_name=new_client.full_name,
                                   company_name=new_client.company_name, hashed_password=hashed_password,
                                   disabled=new_client.disabled)
        response.message = "Sign up for new client with client_id {client_id} is successful.".format(
            client_id=new_client.client_id)
        response.status = status.HTTP_200_OK

    return response


@app.post("/token", response_model=Token)
async def login_and_get_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_client(app.rds_data_store, form_data.username, form_data.password)
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


@app.get("/get_client_details", response_model=ResponseMessage)
async def add_shopify_credentials_to_logged_in_client(*, current_client: LoggedinClient = Depends(
    get_current_active_client)):
    response = ResponseMessage()
    response.message = current_client.client_id
    response.status = status.HTTP_200_OK
    return response


@app.post("/add_shopify_credentials", response_model=ResponseMessage)
async def add_shopify_credentials_to_logged_in_client(*, current_client: LoggedinClient = Depends(
    get_current_active_client),
                                                      shopify_credentials: ShopifyCredential):
    ClientAgent.add_shopify_credentials_to_existing_client(data_store=app.rds_data_store,
                                                           client_id=current_client.client_id,
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
    experiment = ExperimentAgent.create_experiment_for_client_id(data_store=app.rds_data_store,
                                                                 client_id=current_client.client_id,
                                                                 experiment_name=new_experiment.experiment_name,
                                                                 page_type=new_experiment.page_type,
                                                                 experiment_type=new_experiment.experiment_type,
                                                                 status=new_experiment.status,
                                                                 created_on_timestamp=new_experiment.created_on_timestamp,
                                                                 last_updated_on_timestamp=new_experiment.last_updated_on_timestamp)
    return experiment


@app.get("/list_experiments", response_model=List[Experiment])
async def list_experiments(*, current_client: ShopifyClient = Depends(get_current_active_client)):
    experiment_ids = ExperimentAgent.get_experiments_for_client_id(data_store=app.rds_data_store,
                                                                   client_id=current_client.client_id)
    return experiment_ids


@app.post("/add_variation", response_model=Variation)
async def add_variation(*, current_client: ShopifyClient = Depends(get_current_active_client),
                        new_variation: NewVariation):
    variation = VariationAgent.create_variation_for_client_id_and_experiment_id(data_store=app.rds_data_store,
                                                                                client_id=current_client.client_id,
                                                                                experiment_id=new_variation.experiment_id,
                                                                                variation_name=new_variation.variation_name,
                                                                                traffic_percentage=new_variation.traffic_percentage)
    return variation


@app.post("/get_variation_id_to_redirect", response_model=Variation)
async def get_variation_id_to_redirect(*, recommendation_request: RecommendationRequest):
    variation = VariationAgent.get_variation_id_to_recommend(data_store=app.rds_data_store,
                                                             client_id=recommendation_request.client_id,
                                                             experiment_id=recommendation_request.experiment_id,
                                                             session_id=recommendation_request.session_id)
    creation_time = DateUtils.get_timestamp_now()
    EventAgent.register_event_for_client(data_store=app.rds_data_store, client_id=recommendation_request.client_id,
                                         experiment_id=recommendation_request.experiment_id,
                                         session_id=recommendation_request.session_id,
                                         variation_id=variation["variation_id"], event_name="served",
                                         creation_time=creation_time)

    return variation


@app.post("/register_event", response_model=ResponseMessage)
async def register_event(*, event: Event):
    creation_time = DateUtils.get_timestamp_now()
    EventAgent.register_event_for_client(data_store=app.rds_data_store, client_id=event.client_id,
                                         experiment_id=event.experiment_id,
                                         session_id=event.session_id, variation_id=event.variation_id,
                                         event_name=event.event_name, creation_time=creation_time)

    response = ResponseMessage()
    response.message = "Event registration for client_id {client_id} is successful.".format(
        client_id=event.client_id)
    response.status = status.HTTP_200_OK
    return response


@app.post("/get_session_count_for_dashboard", response_model=dict)
async def get_session_count_for_dashboard(*, current_client: ShopifyClient = Depends(get_current_active_client),
                                          experiment_id: str):
    result = DashboardAgent.get_session_count_per_variation_over_time(data_store=app.rds_data_store,
                                                                      client_id=current_client.client_id,
                                                                      experiment_id=experiment_id)

    return result


@app.post("/get_visitor_count_for_dashboard", response_model=dict)
async def get_visitor_count_for_dashboard(*, current_client: ShopifyClient = Depends(get_current_active_client),
                                          experiment_id: str):
    result = DashboardAgent.get_visitor_count_per_variation_over_time(data_store=app.rds_data_store,
                                                                      client_id=current_client.client_id,
                                                                      experiment_id=experiment_id)

    return result


@app.post("/get_conversion_rate_for_dashboard", response_model=dict)
async def get_conversion_rate_for_dashboard(*, current_client: ShopifyClient = Depends(get_current_active_client),
                                            experiment_id: str):
    result = DashboardAgent.get_conversion_rate_per_variation_over_time(data_store=app.rds_data_store,
                                                                        client_id=current_client.client_id,
                                                                        experiment_id=experiment_id)

    return result


@app.post("/get_conversion_table_for_dashboard", response_model=List[dict])
async def get_conversion_table_for_dashboard(*, current_client: ShopifyClient = Depends(get_current_active_client),
                                             experiment_id: str):
    result = DashboardAgent.get_conversion_rate_of_experiment(data_store=app.rds_data_store,
                                                              client_id=current_client.client_id,
                                                              experiment_id=experiment_id)

    return result


@app.post("/get_experiment_summary", response_model=dict)
async def get_experiment_summary(*, current_client: ShopifyClient = Depends(get_current_active_client),
                                 experiment_id: str):
    result = dict()
    result["status"] = "Variation Blue is winning. It is 6% better than the others."
    result[
        "conclusion"] = "There is NOT enough evidence to conclude the experiment (It is NOT yet statistically significant)."
    result["recommendation"] = "Recommendation: CONTINUE the Experiment."
    return result


@app.post("/register_visit", response_model=ResponseMessage)
async def register_visit(*, visit: Visit):
    creation_time = DateUtils.get_timestamp_now()
    VisitAgent.register_visit_for_client(data_store=app.rds_data_store, client_id=visit.client_id,
                                         session_id=visit.session_id,
                                         event_name=visit.event_name, creation_time=creation_time)

    response = ResponseMessage()
    response.message = "Visit registration for client_id {client_id} and event name {event_name} is successful.".format(
        client_id=visit.client_id, event_name=visit.event_name)
    response.status = status.HTTP_200_OK
    return response
