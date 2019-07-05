with open("globals", "r") as f:
    for var in f.readlines():
        print(var.replace("\n", ""))