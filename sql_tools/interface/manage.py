#!/usr/bin/env python
import os
import sys

from django.core.management import call_command, execute_from_command_line
from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "interface.settings")


def main():
    execute_from_command_line(sys.argv)


def serve():
    get_wsgi_application()
    call_command("runserver", "3400")

if __name__ == "__main__":
    main()
