# General Information
1) Inside app we have **config.yaml** with i/p, o/p paths
2) Results are will be stores in **app/Result** on Runtime
3) Data.zip will be automatically unzipped in **app/Data** on Runtime
4) **app/src** is compiled.

# Steps to Run
Clone the repo and follow these steps:
1. Clone the repoitory
2. Go to the Project Directory and run $ **cd app && spark-submit --master "local[*]" --py-files src.zip --files config.yaml main.py && cd ..**
