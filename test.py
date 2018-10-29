import json


def main():
    my_json = {"tenant": "matheus"}
    my_string = json.dumps(my_json)
    print(my_string)
    
if __name__=="__main__":
    main()