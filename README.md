# binaize-optimize

## NEVER EVER COMMIT THE FILES ```config.py``` or ```config.env```

## For Local development

1. Install python 3.6.8

2. Install the dependencies

    ```
    pip3 install -r requirements.txt
    ```
   
3. add ```config.py``` in the root folder of the project.

4. To start the web server

    ``` 
    uvicorn optimization_platform.deployment.server:app --reload 
    ```
   
5. Go to http://127.0.0.1:8000/docs

## Using docker and docker-compose

1. add ```config.env``` in the root folder of the project.

2. docker-compose -f docker-compose-optim.yaml build

3. docker-compose -f docker-compose-optim.yaml up

4. Go to http://127.0.0.1:6006/docs

## To run the test cases

1. docker-compose -f docker-compose-test.yaml build

2. docker-compose -f docker-compose-test.yaml up

    
    The output should be 

    ```bash
    ----------------------------------------------------------------------
    Ran X tests in 0.001s
    
    OK
    
    ```
   
## To reset the tables on AWS RDS

```bash
bash initialize_rds.sh
```


