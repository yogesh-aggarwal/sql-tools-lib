from django.shortcuts import render

# Create your views here.

def index(req):
    return render(req, "execute/index.html", {"connected": 1})
