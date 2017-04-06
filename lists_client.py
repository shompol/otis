#!python
from urllib.request import urlopen
print(urlopen("http://localhost:5000/lists/omn/").read().decode())
