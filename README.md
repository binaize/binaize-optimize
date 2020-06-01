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

## To deploy in EC2

```bash
ssh -i "binaize-optimize.pem" ubuntu@34.201.173.41
sudo apt update
sudo apt -y install docker.io
sudo apt -y install docker-compose
git clone https://github.com/binaize/binaize-optimize.git
cd binaize-optimize
git checkout development
scp -i "binaize-optimize.pem" ./optim.env ubuntu@api.binaize.com:~/binaize-optimize/
nohup sudo docker-compose -f docker-compose-optim.yaml up --build --remove-orphans >> ~/optim.out&
```

## How to deploy to ECS

1. Enter access key, secret key and region us-east-1

    ```
    aws configure
    ```

2. Create repository to upload docker image

	```
	aws ecr create-repository --repository-name binaize-optim-repo
    ```

3. Build the docker image locally

    ```
    docker build -t binaize-optim -f Dockerfile.optim .
    ```

4. Tag the docker image locally

    ```
    docker tag binaize-optim 117859797117.dkr.ecr.us-east-1.amazonaws.com/binaize-optim-repo
    ```

5. Login into ecr

    ```
    aws ecr get-login-password | docker login --username AWS --password-stdin 117859797117.dkr.ecr.us-east-1.amazonaws.com
    ```

6. Push locally built image to repository

    ```
    docker push 117859797117.dkr.ecr.us-east-1.amazonaws.com/binaize-optim-repo
    ```

7. Configure ecs-cli

    ```
    ecs-cli configure --cluster binaize-optimize-cluster --default-launch-type EC2 --config-name binaize-optimize --region us-east-1
    ```

8. Create ecs-cli profile

    ```
    ecs-cli configure profile --access-key AWS_ACCESS_KEY_ID --secret-key AWS_SECRET_ACCESS_KEY --profile-name  binaize-optimize-profile
    ```
   
9. Bring up the cluster

    ```
    ecs-cli up --keypair binaize-optimize --capability-iam --size 2 --instance-type t2.medium --cluster-config binaize-optimize --ecs-profile binaize-optimize-profile
    ```
   
10. Make sure ecs-params.yaml is present in the working directory

    ```
    ecs-cli compose --file docker-compose-aws.yaml up --create-log-groups --cluster-config binaize-optimize  --ecs-profile binaize-optimize-profile
    ```
    
11. 







