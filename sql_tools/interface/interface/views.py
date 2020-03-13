from django.shortcuts import render


def about(req):
    return render(req, "about/index.html")
