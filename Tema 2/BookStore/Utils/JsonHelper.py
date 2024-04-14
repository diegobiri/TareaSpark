import json

# Function to create a JSON file
def create_json_file(filename, data):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)

# Function to read and return data from a JSON file
def read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        return []

# Function to modify an existing JSON file
def modify_json_file(filename, new_data):
    existing_data = read_json_file(filename)
    if existing_data is not None:
        existing_data.update(new_data)
        create_json_file(filename, existing_data)
        print(f'File {filename} modified.')
    else:
        print(f'File {filename} does not exist.')

# Function to delete a JSON file
def delete_json_file(filename):
    try:
        import os
        os.remove(filename)
        print(f'File {filename} deleted.')
    except FileNotFoundError:
        print(f'File {filename} does not exist.')
