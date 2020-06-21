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

docker-compose -f docker-compose-optim.yaml up --build --remove-orphans

The output should be 
```
----------------------------------------------------------------------     
Ran X tests in 0.001s          
OK
```

## To reset the tables on AWS RDS

```bash
bash initialize_rds.sh
```

## To deploy in EC2 DEV cluster

```bash
ssh -i "binaize-optimize.pem" ubuntu@dev.api.binaize.com
sudo apt update
sudo apt -y install docker.io
sudo apt -y install docker-compose
git clone https://github.com/binaize/binaize-optimize.git
cd binaize-optimize
git checkout development
scp -i "binaize-optimize.pem" ./optim-dev.env ubuntu@dev.api.binaize.com:~/binaize-optimize/
cp binaize-optimize/optim-dev.env binaize-optimize/optim.env
```

## To deploy in EC2 STAGING cluster

```bash
ssh -i "binaize-optimize.pem" ubuntu@staging.api.binaize.com
sudo apt update
sudo apt -y install docker.io
sudo apt -y install docker-compose
git clone https://github.com/binaize/binaize-optimize.git
cd binaize-optimize
git checkout staging
scp -i "binaize-optimize.pem" ./optim-staging.env ubuntu@staging.api.binaize.com:~/binaize-optimize/
cp binaize-optimize/optim-staging.env binaize-optimize/optim.env
```

## To deploy in EC2 PROD cluster

```bash
ssh -i "binaize-optimize.pem" ubuntu@api.binaize.com
sudo apt update
sudo apt -y install docker.io
sudo apt -y install docker-compose
git clone https://github.com/binaize/binaize-optimize.git
cd binaize-optimize
git checkout master
scp -i "binaize-optimize.pem" ./optim-prod.env ubuntu@api.binaize.com:~/binaize-optimize/
cp binaize-optimize/optim-prod.env binaize-optimize/optim.env
```

# For first time deployment

```bash
nohup sudo docker-compose -f docker-compose-optim.yaml up --build --remove-orphans > ~/optim.out&
```

# For re-deployment
```bash
nohup sudo docker-compose -f docker-compose-optim.yaml up --build --remove-orphans optim-server optim-scheduler> ~/optim.out&
```



