import os

def setup():
    os.environ["MY_FLAG"] = "true"
    print("Setup complete")

VALUE = 42