from datetime import datetime, timedelta
from typing import List

import jwt
from fastapi import Depends, HTTPException
from fastapi import FastAPI
from fastapi import status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from fastapi.security import OAuth2PasswordRequestForm
from passlib.context import CryptContext

from config import *
from optimization_platform.deployment.server_models import *
from optimization_platform.src.agents.cookie_agent import CookieAgent
from optimization_platform.src.agents.event_agent import EventAgent
from optimization_platform.src.agents.experiment_agent import ExperimentAgent
from optimization_platform.src.agents.shop_agent import ShopAgent
from optimization_platform.src.agents.variation_agent import VariationAgent
from optimization_platform.src.agents.visit_agent import VisitAgent
from optimization_platform.src.agents.visitor_agent import VisitorAgent
from optimization_platform.src.analytics.conversion.conversion_analytics import ConversionAnalytics
from optimization_platform.src.analytics.experiment.experiment_analytics import ExperimentAnalytics
from optimization_platform.src.analytics.visitor.visitor_analytics import VisitorAnalytics
from utils.data_store.rds_data_store import RDSDataStore
from utils.date_utils import DateUtils
from utils.logger.pylogger import get_logger

logger = get_logger("server", "INFO")

tags_metadata = [
    {
        "name": "Shop",
        "description": "Operations with shops. The **login** logic is also here."
    },
    {
        "name": "Experiment",
        "description": "Operations with experiments."
    },
    {
        "name": "Variation",
        "description": "Operations with variations.",
    },
    {
        "name": "Event",
        "description": "Operations with events.",
    },
    {
        "name": "Visit",
        "description": "Operations with visits.",
    },
    {
        "name": "Cookie",
        "description": "Operations with cookies.",
    },
    {
        "name": "Report",
        "description": "Operations with reports.",
    },
    {
        "name": "Visitor",
        "description": "Operations with visitors.",
    }

]

app = FastAPI(title="Binaize",
              description="Apis for Binaize Optim", docs_url="/bdocs", redoc_url=None,
              version="1.0.0", openapi_tags=tags_metadata, openapi_url="/api/v1/schemas/openapi.json")

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

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/schemas/shop/token")


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def get_shop(data_store, shop_id: str):
    shop_details = ShopAgent.get_shop_details_for_shop_id(data_store=data_store, shop_id=shop_id)
    if shop_details is not None:
        return BinaizeShop(**shop_details)


def authenticate_shop(data_store, shop_id: str, password: str):
    shop = get_shop(data_store, shop_id)
    if not shop:
        return False
    if not verify_password(password, shop.hashed_password):
        return False
    return shop


def create_access_token(*, data: dict, expires_delta: timedelta):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def _get_current_shop(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        shop_id = payload.get("sub")
        user = get_shop(app.rds_data_store, shop_id=shop_id)
        if user is None:
            raise credentials_exception
        return user
    except Exception:
        raise credentials_exception


async def get_current_active_shop(current_shop: BinaizeShop = Depends(_get_current_shop)):
    if current_shop.disabled:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Inactive user")
    return current_shop


@app.get("/", response_model=dict)
async def home_page():
    import time
    t = time.time()
    response = dict()
    response["message"] = app.description
    response["status"] = status.HTTP_200_OK
    logger.info("prod info message")
    logger.info(time.time() - t)
    return response


@app.post("/api/v1/schemas/shop/sign_up", response_model=ResponseMessage, tags=["Shop"],
          summary="Sign up a new shop")
async def sign_up_new_shop(shop: ShopifyShop):
    """
        Sign up a new shop:
        - **shop_id**: shopify store domain name
        - **shopify_access_token**: shopify access token of the new shop
    """

    logger.info("{hash}".format(hash="".join(["#" for i in range(60)])))
    logger.info("signing up new shop started.")

    logger.info("shop id : {shop_id}".format(shop_id=shop.shop_id))
    logger.info(
        "shopify_access_token : {shopify_access_token}".format(shopify_access_token=shop.shopify_access_token))

    response = ResponseMessage()
    creation_time = DateUtils.get_timestamp_now()
    hashed_password = get_password_hash(shop.shop_id)
    sign_up_status = ShopAgent.add_new_shop(data_store=app.rds_data_store, shop_id=shop.shop_id,
                                            shopify_access_token=shop.shopify_access_token,
                                            hashed_password=hashed_password, creation_timestamp=creation_time)
    if sign_up_status is None:
        response.message = "sign up for new shop with shop id {shop_id} failed.".format(
            shop_id=shop.shop_id)
    else:
        response.message = "sign up for new shop with shop id {shop_id} is successful.".format(
            shop_id=shop.shop_id)
    response.status = status.HTTP_200_OK

    logger.info(response.message)
    logger.info("signing up new shop ended.")
    logger.info("{hash}".format(hash="".join(["#" for i in range(60)])))

    return response


@app.post("/api/v1/schemas/shop/delete", response_model=ResponseMessage, tags=["Shop"],
          summary="Remove an existing shop")
async def delete_shop(*, shop: ShopifyShop):
    """
        Delete a new shop:
        - **shop_id**: shopify store domain name
    """

    logger.info("{hash}".format(hash="".join(["#" for i in range(60)])))
    logger.info("deletion of existing shop started.")
    logger.info("shop id : {shop_id}".format(shop_id=shop.shop_id))
    logger.info(
        "shopify_access_token : {shopify_access_token}".format(shopify_access_token=shop.shopify_access_token))

    response = ResponseMessage()
    ShopAgent.delete_shop_for_shop_id(data_store=app.rds_data_store, shop_id=shop.shop_id)
    response.message = "deletion for shop with shop id {shop_id} is successful.".format(
        shop_id=shop.shop_id)
    response.status = status.HTTP_200_OK

    logger.info(response.message)
    logger.info("deletion of existing shop ended.")
    logger.info("{hash}".format(hash="".join(["#" for i in range(60)])))

    return response


@app.post("/api/v1/schemas/shop/token", response_model=Token, tags=["Shop"], summary="Login and get access token")
async def login_and_get_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """
        The shop logs in and get an access token for the session:
        - **username**: shop_id of the shop while signing up
        - **password**: password used for signing up by the shop
    """

    shop = authenticate_shop(data_store=app.rds_data_store, shop_id=form_data.username, password=form_data.password)
    if not shop:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": shop.shop_id}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/api/v1/schemas/shop/details", response_model=Shop, tags=["Shop"],
         summary="Get details of a logged in shop")
async def get_shop_details(*, shop: BinaizeShop = Depends(
    get_current_active_shop)):
    """
        Get details of a logged in shop:
        - **access_token**: access token issued by the server to the logged in shop
    """
    shop = Shop(shop_id=shop.shop_id, shop_domain=shop.shop_domain, shop_owner=shop.shop_owner, email_id=shop.email_id,
                city=shop.city, country=shop.country, province=shop.province, disabled=shop.disabled,
                creation_time=shop.creation_time, timezone=shop.timezone)
    return shop


@app.get("/api/v1/schemas/shop/shopify_details", response_model=dict, tags=["Shop"],
         summary="Get details of a registered shop")
async def get_shopify_details(*, shop_id: str):
    """
        Get details of a registered shop:
        - **shop_id**: shopify store domain name
    """
    shopify_details = ShopAgent.get_shopify_details_for_shop_id(data_store=app.rds_data_store, shop_id=shop_id)
    return shopify_details


@app.post("/api/v1/schemas/shop/nonce/update", response_model=ResponseMessage, tags=["Shop"],
          summary="Update nonce of shop")
async def update_nonce(*, shop_nonce: ShopNonce):
    """
        Get details of a registered shop:
        - **shop_id**: shopify store domain name
    """
    response = ResponseMessage()
    upsert_status = ShopAgent.upsert_shopify_nonce_for_shop_id(data_store=app.rds_data_store,
                                                               shop_id=shop_nonce.shop_id,
                                                               shopify_nonce=shop_nonce.shopify_nonce)
    if upsert_status is not None:
        response.message = "updation of nonce {shopify_nonce} for shop with shop id {shop_id} is successful.".format(
            shopify_nonce=shop_nonce.shopify_nonce, shop_id=shop_nonce.shop_id)
        response.status = status.HTTP_200_OK
    else:
        response.message = "updation of nonce {shopify_nonce} for shop with shop id {shop_id} failed.".format(
            shopify_nonce=shop_nonce.shopify_nonce, shop_id=shop_nonce.shop_id)
        response.status = status.HTTP_424_FAILED_DEPENDENCY
    return response


@app.post("/api/v1/schemas/experiment/create", response_model=Experiment, tags=["Experiment"],
          summary="Create a new experiment")
async def add_experiment(*, current_client: BinaizeShop = Depends(get_current_active_shop),
                         new_experiment: BaseExperiment):
    """
        Create a new experiment for a logged in client:
        - **access_token**: access token issued by the server to the logged in client
        - **experiment_name**: name of the experiment
        - **page_type**: which page the experiment is for - *home*/*product*
        - **experiment_type**: type of the experiment - *multi-variate*/*ab-test*/*personalization*
        - **status**: status of the experiment - *active*/*archived*/*done*
    """
    creation_time = DateUtils.get_timestamp_now()
    last_updation_time = DateUtils.get_timestamp_now()
    experiment = ExperimentAgent.create_experiment_for_client_id(data_store=app.rds_data_store,
                                                                 client_id=current_client.client_id,
                                                                 experiment_name=new_experiment.experiment_name,
                                                                 page_type=new_experiment.page_type,
                                                                 experiment_type=new_experiment.experiment_type,
                                                                 status=new_experiment.status,
                                                                 creation_time=creation_time,
                                                                 last_updation_time=last_updation_time)
    return experiment


@app.get("/api/v1/schemas/experiment/list", response_model=List[Experiment], tags=["Experiment"],
         summary="List down all the experiments")
async def list_experiments(*, current_client: BinaizeShop = Depends(get_current_active_shop)):
    """
        List down all the experiments for a logged in client:
        - **access_token**: access token issued by the server to the logged in client
    """
    experiment_ids = ExperimentAgent.get_experiments_for_client_id(data_store=app.rds_data_store,
                                                                   client_id=current_client.client_id)
    return experiment_ids


@app.post("/api/v1/schemas/variation/create", response_model=Variation, tags=["Variation"],
          summary="Create a new variation")
async def add_variation(*, current_client: BinaizeShop = Depends(get_current_active_shop),
                        new_variation: NewVariation):
    """
        Create a new variation for an existing experiment of a logged in client:
        - **access_token**: access token issued by the server to the logged in client
        - **experiment_id**: id of the experiment
        - **variation_name**: name of the variation
        - **traffic_percentage**: percentage of traffic to be redirected to this variation
    """
    variation = VariationAgent.create_variation_for_client_id_and_experiment_id(data_store=app.rds_data_store,
                                                                                client_id=current_client.client_id,
                                                                                experiment_id=new_variation.experiment_id,
                                                                                variation_name=new_variation.variation_name,
                                                                                traffic_percentage=new_variation.traffic_percentage)
    return variation


@app.get("/api/v1/schemas/variation/redirection", response_model=Variation, tags=["Variation"],
         summary="Get the variation id to be redirected to")
async def get_variation_id_to_redirect(*, client_id: str, experiment_id: str, session_id: str):
    """
        Get the variation id to be redirected to when a visitor visits the client's website:
        - **client_id**: the e-mail id of the new client
        - **experiment_id**: id of the experiment
        - **session_id**: shopify_y attribute of shopify cookie
    """
    variation = VariationAgent.get_variation_id_to_recommend(data_store=app.rds_data_store,
                                                             client_id=client_id,
                                                             experiment_id=experiment_id,
                                                             session_id=session_id)
    creation_time = DateUtils.get_timestamp_now()
    EventAgent.register_event_for_client(data_store=app.rds_data_store, client_id=client_id,
                                         experiment_id=experiment_id,
                                         session_id=session_id,
                                         variation_id=variation["variation_id"], event_name="served",
                                         creation_time=creation_time)

    return variation


@app.post("/api/v1/schemas/event/register", response_model=ResponseMessage, tags=["Event"], summary="Register event")
async def register_event(*, event: Event):
    """
        Register conversion event when a visitor visits the client's website:
        - **client_id**: the e-mail id of the new client
        - **experiment_id**: id of the experiment
        - **session_id**: shopify_y attribute of shopify cookie
        - **variation_id**: id of the variation
        - **event_name**: name of the conversion event
    """

    creation_time = DateUtils.get_timestamp_now()
    result = EventAgent.register_event_for_client(data_store=app.rds_data_store, client_id=event.client_id,
                                                  experiment_id=event.experiment_id,
                                                  session_id=event.session_id, variation_id=event.variation_id,
                                                  event_name=event.event_name, creation_time=creation_time)

    response = ResponseMessage()
    response.message = "Event registration for client_id {client_id} is successful.".format(
        client_id=event.client_id)
    response.status = status.HTTP_200_OK
    if result is None:
        response.message = "Event registration for client_id {client_id} failed.".format(
            client_id=event.client_id)
        response.status = status.HTTP_404_NOT_FOUND

    return response


@app.post("/api/v1/schemas/visit/register", response_model=ResponseMessage, tags=["Visit"], summary="Register visit")
async def register_visit(*, visit: Visit):
    """
        Register visit event when a visitor visits the client's website:
        - **client_id**: the e-mail id of the new client
        - **session_id**: shopify_y attribute of shopify cookie
        - **event_name**: name of the visit event
        - **url**: url visited by the website visitor
    """
    creation_time = DateUtils.get_timestamp_now()
    VisitAgent.register_visit_for_client(data_store=app.rds_data_store, client_id=visit.client_id,
                                         session_id=visit.session_id,
                                         event_name=visit.event_name, creation_time=creation_time, url=visit.url)

    response = ResponseMessage()
    response.message = "Visit registration for client_id {client_id} and event name {event_name} is successful.".format(
        client_id=visit.client_id, event_name=visit.event_name)
    response.status = status.HTTP_200_OK
    return response


@app.post("/api/v1/schemas/visitor/register", response_model=ResponseMessage, tags=["Visitor"],
          summary="Register visitor")
async def register_visitor(*, visitor: Visitor):
    """
        Register visitors on the client's website:
    """
    creation_time = DateUtils.get_timestamp_now()
    VisitorAgent.register_visitor_for_client(data_store=app.rds_data_store, client_id=visitor.client_id,
                                             session_id=visitor.session_id, ip=visitor.ip,
                                             city=visitor.city, region=visitor.region, country=visitor.country,
                                             lat=visitor.lat, long=visitor.long, timezone=visitor.timezone,
                                             browser=visitor.browser, os=visitor.os, device=visitor.device,
                                             fingerprint=visitor.fingerprint,
                                             creation_time=creation_time)

    response = ResponseMessage()
    response.message = "Visitor registration for client_id {client_id} and ip {ip} is successful.".format(
        client_id=visitor.client_id, ip=visitor.ip)
    response.status = status.HTTP_200_OK
    return response


@app.post("/api/v1/schemas/cookie/register", response_model=ResponseMessage, tags=["Cookie"],
          summary="Register cookie information")
async def register_cookie(*, cookie: Cookie):
    """
        Register cookie information when a visitor visits the client's website::
        - **client_id**: the e-mail id of the new client
        - **session_id**: shopify_y attribute of shopify cookie
        - **cart_token**: cart_token attribute of shopify cookie
    """
    creation_time = DateUtils.get_timestamp_now()
    CookieAgent.register_cookie_for_client(data_store=app.rds_data_store, client_id=cookie.client_id,
                                           session_id=cookie.session_id, cart_token=cookie.cart_token,
                                           creation_time=creation_time)
    experiment_id = ExperimentAgent.get_latest_experiment_id(data_store=app.rds_data_store, client_id=cookie.client_id)
    if experiment_id is not None:
        variation = VariationAgent.get_variation_id_to_recommend(data_store=app.rds_data_store,
                                                                 client_id=cookie.client_id,
                                                                 experiment_id=experiment_id,
                                                                 session_id=cookie.session_id)
        if variation is not None:
            EventAgent.register_event_for_cookie(data_store=app.rds_data_store, client_id=cookie.client_id,
                                                 experiment_id=experiment_id, session_id=cookie.session_id,
                                                 event_name="served", creation_time=creation_time,
                                                 variation_id=variation["variation_id"])

    response = ResponseMessage()
    response.message = "Cookie registration for client_id {client_id} is successful.".format(
        client_id=cookie.client_id)
    response.status = status.HTTP_200_OK
    return response


@app.get("/api/v1/schemas/report/conversion-over-time", response_model=dict, tags=["Report"],
         summary="Get conversion rate")
async def get_goal_conversion_for_dashboard(*, current_client: BinaizeShop = Depends(get_current_active_shop),
                                            experiment_id: str):
    """
        Get session count, visitor count, goal conversion count, goal conversion rate, sales conversion count and sales
        conversion rate of all the variations of an existing experiment of a logged in client for last 7 days at a
        daily level:
        - **access_token**: access token issued by the server to the logged in client
        - **experiment_id**: id of the experiment
    """
    result = ExperimentAnalytics.get_conversion_per_variation_over_time(data_store=app.rds_data_store,
                                                                        client_id=current_client.client_id,
                                                                        experiment_id=experiment_id,
                                                                        timezone_str=current_client.client_timezone)

    return result


@app.get("/api/v1/schemas/report/conversion-table", response_model=List[dict], tags=["Report"],
         summary="Get conversion table")
async def get_conversion_table_for_dashboard(*, current_client: BinaizeShop = Depends(get_current_active_shop),
                                             experiment_id: str):
    """
        Get conversion table of all the variations of an existing experiment of a logged in client till now:
        - **access_token**: access token issued by the server to the logged in client
        - **experiment_id**: id of the experiment
    """
    result = ExperimentAnalytics.get_conversion_table_of_experiment(data_store=app.rds_data_store,
                                                                    client_id=current_client.client_id,
                                                                    experiment_id=experiment_id)

    return result


@app.get("/api/v1/schemas/report/experiment-summary", response_model=dict, tags=["Report"],
         summary="Get experiment summary")
async def get_experiment_summary(*, current_client: BinaizeShop = Depends(get_current_active_shop),
                                 experiment_id: str):
    """
        Get experiment summary of an existing experiment of a logged in client:
        - **access_token**: access token issued by the server to the logged in client
        - **experiment_id**: id of the experiment
    """

    result = ExperimentAnalytics.get_summary_of_experiment(data_store=app.rds_data_store,
                                                           client_id=current_client.client_id,
                                                           experiment_id=experiment_id)

    return result


@app.get("/api/v1/schemas/report/shop-funnel", response_model=dict, tags=["Report"],
         summary="Get shop funnel analytics")
async def get_shop_funnel_analytics_for_dashboard(*,
                                                  current_client: BinaizeShop = Depends(get_current_active_shop),
                                                  start_date: str, end_date: str):
    """
        Get shop funnel analytics of the client's website till now:
        - **access_token**: access token issued by the server to the logged in client
        - **start_date**: start date in YYYY-MM-DD format
        - **end_date**: end date in YYYY-MM-DD format
    """
    result = ConversionAnalytics.get_shop_funnel_analytics(data_store=app.rds_data_store,
                                                           client_id=current_client.client_id,
                                                           start_date_str=start_date, end_date_str=end_date,
                                                           timezone_str=current_client.client_timezone)

    return result


@app.get("/api/v1/schemas/report/product-conversion", response_model=dict, tags=["Report"],
         summary="Get product conversion analytics")
async def get_product_conversion_analytics_for_dashboard(*, current_client: BinaizeShop = Depends(
    get_current_active_shop), start_date: str, end_date: str):
    """
        Get product conversion analytics of the client's website till now:
        - **access_token**: access token issued by the server to the logged in client
        - **start_date**: start date in YYYY-MM-DD format
        - **end_date**: end date in YYYY-MM-DD format
    """
    result = ConversionAnalytics.get_product_conversion_analytics(data_store=app.rds_data_store,
                                                                  client_id=current_client.client_id,
                                                                  start_date_str=start_date, end_date_str=end_date,
                                                                  timezone_str=current_client.client_timezone)

    return result


@app.get("/api/v1/schemas/report/landing-page", response_model=dict, tags=["Report"],
         summary="Get landing page analytics")
async def get_landing_page_analytics_for_dashboard(*,
                                                   current_client: BinaizeShop = Depends(get_current_active_shop),
                                                   start_date: str, end_date: str
                                                   ):
    """
        Get landing page analytics of the client's website till now:
        - **access_token**: access token issued by the server to the logged in client
        - **start_date**: start date in YYYY-MM-DD format
        - **end_date**: end date in YYYY-MM-DD format
    """
    result = ConversionAnalytics.get_landing_page_analytics(data_store=app.rds_data_store,
                                                            client_id=current_client.client_id,
                                                            start_date_str=start_date, end_date_str=end_date,
                                                            timezone_str=current_client.client_timezone)

    return result


@app.get("/api/v1/schemas/report/visitor-activity", response_model=dict, tags=["Report"],
         summary="Get visitor analytics")
async def get_landing_page_analytics_for_dashboard(*,
                                                   current_client: BinaizeShop = Depends(get_current_active_shop),
                                                   start_date: str, end_date: str
                                                   ):
    """
        Get visitor analytics of the client's website till now:
        - **access_token**: access token issued by the server to the logged in client
        - **start_date**: start date in YYYY-MM-DD format
        - **end_date**: end date in YYYY-MM-DD format
    """
    result = VisitorAnalytics.get_sales_analytics(data_store=app.rds_data_store,
                                                  client_id=current_client.client_id,
                                                  start_date_str=start_date, end_date_str=end_date,
                                                  timezone_str=current_client.client_timezone)

    return result
