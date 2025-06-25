
import os
import json

password = "admin123"  # Hardcoded password (Bandit warning)

def connect_to_db():
    print("Connecting to DB with password:", password)

def insecure_file_read():
    filename = input("Enter file name: ")
    with open(filename, "r") as f:
        print(f.read())  # Unvalidated file input (Bandit warning)

def run_command():
    command = input("Enter command: ")
    os.system(command)  # Command injection risk (Bandit warning)

def unused_function():
    secret_key = "super_secret"  # Unused variable
    pass

connect_to_db()
insecure_file_read()
run_command()
