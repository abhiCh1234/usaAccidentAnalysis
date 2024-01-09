# General Information
1) Inside app we have **config.yaml** with i/p, o/p paths
2) Results are will be stores in **Data/Result** on Runtime
3) Data.zip will be automatically unzipped in **Data** on Runtime

# Steps to Execute
Clone the repo and follow these steps:
1. Clone the repoitory
2. Go to the Project Directory and run $ **cd app && spark-submit --master "local[*]" main.py && cd ..**
3. Or in **app** folder run $ **spark-submit --master "local[*]" main.py**
