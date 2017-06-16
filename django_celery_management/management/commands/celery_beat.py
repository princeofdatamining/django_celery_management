from django.core.management import base
from django.conf import settings

from django_celery_management import utils


class Command(base.BaseCommand):

    help = "django_celery_beat management tools"

    def add_arguments(self, parser):
        """
        ... list
            # list all task
        ... load
            # load tasks and append to database
        ... ID
            # get task by ID
        ... TASK
            # get task by TASK
        ... NAME
            # get task by NAME
        """
        parser.add_argument('action', nargs='?', help='list|get|load')

    def handle(self, *args, **options):
        action = options.get('action')
        if not action:
            return
        method = 'do_{}'.format(action)
        if hasattr(self, method):
            return getattr(self, method)()
        self.do_get(action)

    def do_list(self):
        utils.list_task()

    def do_get(self, arg):
        if arg.isdigit():
            utils.get_task(id=arg)
        elif '.' in arg:
            utils.get_task(task=arg)
        else:
            utils.get_task(name=arg)

    def do_load(self, filename=None):
        utils.load_tasks(filename)
