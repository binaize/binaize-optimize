# binaize-optimize

## NEVER EVER COMMIT THE FILE ```config.py```

1. Install python 3.6.8

2. Install the dependencies

    ```
    pip3 install -r requirements.txt
    ```

3. To start the web server

    ``` 
    uvicorn optimization_platform.deployment.server:app --reload 
    ```
   
4. Go to http://127.0.0.1:8000/docs